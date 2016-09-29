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
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import groovyx.gpars.ParallelEnhancer

@Grapes([
        @Grab("com.amazonaws:aws-java-sdk:1.11.36"),
        @Grab(group = 'org.elasticsearch', module = 'elasticsearch', version = '2.4.1'),
        @Grab(group = 'org.codehaus.gpars', module = 'gpars', version = '1.2.1')
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
        processLogs()
    }

    private def processLogs() {
        awsLogsClient = new AWSLogsClient()
        ParallelEnhancer.enhanceInstance(instances)
        instances.eachParallel { String i -> processLogs(i) }
    }

    private def processLogs(String instance) {
        println("Processing logs for instance $instance")

        GetLogEventsRequest request = new GetLogEventsRequest(logGroup, instance)
        if (from) request.setStartTime(from)
        if (to) request.setEndTime(to)
        if (lastMinutes) {
            use(TimeCategory) {
                request.setStartTime((new Date() - lastMinutes.minutes).time)
            }
        }
        request.setStartFromHead(true)

        def oldToken
        while (true) {
            println("Extract and load logs for $instance with token ${oldToken ?: "None"}")
            def eventsResult = getLogEvents(request)
            BulkRequestBuilder bulk = esClient.prepareBulk()
            eventsResult.events.each { evt ->
                evt.message.split("\n").each { m ->
                    IndexRequest indexRequest = new IndexRequest(ec2Name, "log")
                            .source([timestamp: evt.timestamp, message: m, instance: instance])
                    bulk.add(indexRequest)
                }
            }
            if (bulk.numberOfActions()) {
                bulk.execute().actionGet()
            }

            if (!eventsResult.nextForwardToken || eventsResult.nextForwardToken == oldToken) break
            oldToken = eventsResult.nextForwardToken
            request.setNextToken(eventsResult.nextForwardToken)
            request.setStartTime(null)
            request.setEndTime(null)
        }
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
        Settings.Builder settings = Settings.settingsBuilder()
        settings.put("cluster.name", clusterName)
        settings.put("client.transport.sniff", false)
        esClient = TransportClient.builder().settings(settings).build()
        esClient.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(elasticHost, elasticPort)))

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
                                .field("type", "string")
                            .endObject()
                            .startObject("instance")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
        createIndexRequestBuilder.addMapping("log", mappingBuilder)

        // MAPPING DONE
        createIndexRequestBuilder.execute().actionGet()
    }
}

def main(String[] args) {
    final String ES_CLUSTER_DEFAULT = 'cloudwatch-cluster'
    final int ES_PORT_DEFAULT = 9300
    final boolean ES_DELETE_DATA_DEFAULT = true

    def cli = new CliBuilder(usage: 'groovy cloudwatch_elk.groovy [options]')
    cli.with {
        a longOpt: 'profile','the AWS profile name to use', args: 1
        n longOpt: 'ec2Name', 'the EC2 name to retrieve instances', args: 1, required: true
        g longOpt: 'logGroup', 'the CloudWatch log group of the instances', args: 1, required: true
        l longOpt: 'lastMinutes', 'specify the number of minutes to extract logs until now', args: 1
        f longOpt: 'from', 'a point in time expressed as dd/MM/yy hh:mm', args: 1
        t longOpt: 'to', 'a point in time expressed as dd/MM/yy hh:mm', args: 1
        i longOpt: 'instances', 'the instances to extract log from, separated by comma', args: Option.UNLIMITED_VALUES, valueSeparator: 'a' as char
        e longOpt: 'elasticHost', 'the Elasticsearch hostname / ip', args: 1, required: true
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
                lastMinutes: options.l ? options.l as int : null,
                elasticHost: options.e,
                elasticPort: options.p ? options.p as int : ES_PORT_DEFAULT,
                clusterName: options.c ?: ES_CLUSTER_DEFAULT,
                deleteData: options.d ? options.d as boolean : ES_DELETE_DATA_DEFAULT)
                .run()
    }
}

main(args)