import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.DescribeInstancesRequest
import com.amazonaws.services.ec2.model.Filter
import com.amazonaws.services.logs.AWSLogsClient
import com.amazonaws.services.logs.model.GetLogEventsRequest
import com.amazonaws.services.logs.model.GetLogEventsResult
import groovy.time.TimeCategory
import org.apache.commons.cli.Option
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.transport.client.PreBuiltTransportClient
import rx.AsyncEmitter
import rx.Observable
import rx.schedulers.Schedulers

import java.util.concurrent.CountDownLatch

@Grapes([
        @Grab(group = 'io.reactivex', module = 'rxjava', version = '1.2.0'),
        @Grab("com.amazonaws:aws-java-sdk:1.11.202"),
        @Grab(group = 'org.elasticsearch', module = 'elasticsearch', version = '6.2.4'),
        @Grab(group = 'org.elasticsearch.client', module = 'transport', version = '6.2.4'),
        @Grab(group = 'org.apache.logging.log4j', module = 'log4j-core', version = '2.9.1')
])

class CloudWatchElasticLoader {
    String ec2Name
    String logGroup
    Long from
    Long to
    List<String> instances
    int lastMinutes

    String elasticHost
    int elasticPort
    String clusterName
    boolean deleteData

    private String awsProfile
    private TransportClient esClient
    private AWSLogsClient awsLogsClient

    def run() {
        setupAWS()
        setupElasticsearch()
        loadInstances()
        processLogsRx()
    }

    private def processLogsRx() {
        awsLogsClient = new AWSLogsClient()

        final CountDownLatch latch = new CountDownLatch(1)
        Observable
                .from(instances)
                .flatMap {
                    Observable.just(it).map { i -> prepareRequest(i) }
                            .flatMap { r -> logEventsResult(r) }
                            .flatMap { t -> esIndexRequests(t.first, t.second) }
                            .subscribeOn(Schedulers.io())
                            .buffer(2000)
                            .map { r -> buildBulk(r) }
                            .filter { b -> b.numberOfActions() > 0 }
        }.subscribe({ b -> b.execute().actionGet() },
                { throwable -> throwable.printStackTrace() },
                { -> latch.countDown() })

        latch.await()
    }

    private Observable<Tuple2<String, GetLogEventsResult>> logEventsResult(GetLogEventsRequest request) {
        Observable.fromEmitter({ emitter ->

            def oldToken
            while (true) {
                println(Thread.currentThread().getName() + " Load logs with request $request")
                GetLogEventsResult eventsResult = getLogEvents(request)
                emitter.onNext(new Tuple2(request.logStreamName, eventsResult))

                if (!eventsResult.nextForwardToken || eventsResult.nextForwardToken == oldToken) break
                oldToken = eventsResult.nextForwardToken
                request.setNextToken(eventsResult.nextForwardToken)
                request.setStartTime(null)
                request.setEndTime(null)
            }

            emitter.onCompleted()
        }, AsyncEmitter.BackpressureMode.BUFFER)
    }

    private Observable<IndexRequest> esIndexRequests(String instance, GetLogEventsResult eventsResult) {
        Observable.fromEmitter({ emitter ->
            println(Thread.currentThread().getName() + " Creating index requests for ${eventsResult.events.size()} events")
            eventsResult.events.each { evt ->
                evt.message.split("\n").each { m ->
                    IndexRequest indexRequest = new IndexRequest(ec2Name, "log")
                            .source([timestamp: evt.timestamp, message: m, instance: instance])
                    emitter.onNext(indexRequest)
                }
            }


            emitter.onCompleted()
        }, AsyncEmitter.BackpressureMode.BUFFER)
    }

    private BulkRequestBuilder buildBulk(List<IndexRequest> requests) {
        BulkRequestBuilder bulk = esClient.prepareBulk()
        requests.forEach { ir -> bulk.add(ir) }
        bulk
    }

    private GetLogEventsRequest prepareRequest(String instance) {
        GetLogEventsRequest request = new GetLogEventsRequest(logGroup, instance)
        if (from) request.setStartTime(from)
        if (to) request.setEndTime(to)
        if (lastMinutes) {
            use(TimeCategory) {
                request.setStartTime((new Date() - lastMinutes.minutes).time)
            }
        }
        request.setStartFromHead(true)
        request
    }

    GetLogEventsResult getLogEvents(GetLogEventsRequest r) {
        try {
            return awsLogsClient.getLogEvents(r)
        } catch (any) {
            println("Unable to process logs for ${r.logStreamName} / ${r.logGroupName} : ${any.message}")
        }
    }

    private def setupAWS() {
        if (awsProfile) {
            System.setProperty('aws.profile', awsProfile)
        }
    }

    private def loadInstances() {
        if (!instances) {
            println("Loading instances from AWS")
            DescribeInstancesRequest instancesRequest = new DescribeInstancesRequest()
            instancesRequest.withFilters([new Filter('tag:Name', [ec2Name])])

            AmazonEC2Client ec2Client = new AmazonEC2Client()
            def instancesResult = ec2Client.describeInstances(instancesRequest)
            instances = instancesResult.reservations.instances.instanceId.flatten() as List<String>
        }
    }


    private def setupElasticsearch() {
        println("Setup Elasticsearch")
        Settings.Builder settings = Settings.builder()
        settings.put("cluster.name", clusterName)
        settings.put("client.transport.sniff", false)
        esClient = new PreBuiltTransportClient(settings.build())
        esClient.addTransportAddress(new TransportAddress(new InetSocketAddress(elasticHost, elasticPort)))

        ClusterHealthResponse actionGet = esClient.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet()
        println("ElasticSearch Health Status $actionGet")

        IndicesExistsResponse res = esClient.admin().indices().prepareExists(ec2Name).execute().actionGet()
        if (res.isExists() && deleteData) {
            println("Previous index found. Deleting it")
            DeleteIndexRequestBuilder delIdx = esClient.admin().indices().prepareDelete(ec2Name)
            delIdx.execute().actionGet()
        }

        CreateIndexRequestBuilder createIndexRequestBuilder = esClient.admin().indices().prepareCreate(ec2Name)

        // MAPPING GOES HERE
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("log")
                        .startObject("properties")
                            .startObject("timestamp")
                                .field("type", "date")
                            .endObject()
                            .startObject("message")
                                .field("type", "text")
                            .endObject()
                            .startObject("instance")
                                .field("type", "keyword")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
        createIndexRequestBuilder.addMapping("log", mappingBuilder)

        // MAPPING DONE
        createIndexRequestBuilder.execute().actionGet()
    }
}

def app_main(String[] args) {
    final ES_HOST_DEFAULT = 'localhost'
    final String ES_CLUSTER_DEFAULT = 'cloudwatch-cluster'
    final int ES_PORT_DEFAULT = 9300
    final boolean ES_DELETE_DATA_DEFAULT = true

    def cli = new CliBuilder(usage: 'groovy cloudwatch_elk.groovy [options]')
    cli.with {
        a longOpt: 'profile', "the AWS profile name to use", args: 1
        n longOpt: 'ec2Name', "the EC2 name to retrieve instances", args: 1, required: true
        g longOpt: 'logGroup', "the CloudWatch log group of the instances", args: 1, required: true
        l longOpt: 'lastMinutes', "specify the number of minutes to extract logs until now", args: 1
        f longOpt: 'from', "a point in time expressed as dd/MM/yy hh:mm", args: 1
        t longOpt: 'to', "a point in time expressed as dd/MM/yy hh:mm", args: 1
        i longOpt: 'instances', "the instances to extract log from, separated by comma", args: Option.UNLIMITED_VALUES, valueSeparator: 'a' as char
        e longOpt: 'elasticHost', "the Elasticsearch hostname / ip, default '$ES_HOST_DEFAULT'", args: 1
        p longOpt: 'elasticPort', "the Elasticsearch port, default '$ES_PORT_DEFAULT'", args: 1
        c longOpt: 'cluster', "the Elasticsearch cluster name, default '$ES_CLUSTER_DEFAULT'", args: 1
        d longOpt: 'deleteData', "when 'true' deletes the index if exists, default '$ES_DELETE_DATA_DEFAULT'", args: 1
    }

    def options = cli.parse(args)

    if (options) {
        new CloudWatchElasticLoader(
                awsProfile: options.a,
                ec2Name: options.n,
                logGroup: options.g,
                from: options.f ? Date.parse("dd/MM/yy hh:mm", options.f).time : null,
                to: options.t ? Date.parse("dd/MM/yy hh:mm", options.t).time : null,
                instances: options.i ?: null,
                lastMinutes: options.l ? options.l as int : 0,
                elasticHost: options.e ?: ES_HOST_DEFAULT,
                elasticPort: options.p ? options.p as int : ES_PORT_DEFAULT,
                clusterName: options.c ?: ES_CLUSTER_DEFAULT,
                deleteData: options.d ? options.d as boolean : ES_DELETE_DATA_DEFAULT)
                .run()
    }
}

app_main(args)