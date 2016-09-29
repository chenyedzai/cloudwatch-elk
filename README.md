# AWS CloudWatch to ELK

```AWS CloudWatch to ELK``` is a command line interface (CLI) to extract events from Amazon CloudWatch logs and load them into a 
local Elasticsearch/Logstash/Kibana (ELK) cluster.
 
CloudWatch logs are not easy to read and analyse. It's not possible to aggregate the logs from different streams or use full text
search capabilities.

The goal of this project is to easy setup an ELK stack and load the logs we need to analyse 'on demand'.

## Requirements

* [Docker](https://docs.docker.com/engine/installation/)
* [Groovy](http://www.groovy-lang.org/download.html)

## Setup

* Clone this repository
* Start the ELK stack ```docker-compose up```
* Access Kibana via web browser ```http://$docker_machine_ip:5601```. If everything is setup correctly you should see the view below

![Kibana Home](/images/kibana.png?raw=true "Kibana Home")

## Load your logs

Use the Groovy CLI to load your logs. Run it without any parameter to see how to use it

```groovy cloudwatch_elk.groovy ```

```shell
usage: groovy cloudwatch_elk.groovy [options]
 -a,--profile <arg>       the AWS profile name to use
 -c,--cluster <arg>       the Elasticsearch cluster name, default
                          'cloudwatch-cluster'
 -d,--deleteData <arg>    when 'true' deletes the index if exists, default
                          'true'
 -e,--elasticHost <arg>   the Elasticsearch hostname / ip
 -f,--from <arg>          a point in time expressed as dd/MM/yy hh:mm
 -g,--logGroup <arg>      the CloudWatch log group of the instances
 -i,--instances <arg>     the instances to extract log from, separated by
                          comma
 -l,--lastMinutes <arg>   specify the number of minutes to extract logs
                          until now
 -n,--ec2Name <arg>       the EC2 name to retrieve instances
 -p,--elasticPort <arg>   the ElasticSearch port, default '9300'
 -t,--to <arg>            a point in time expressed as dd/MM/yy hh:mm
```

### Examples

Load the last hour of logs from all the instances tagged with name 'my-service':

```groovy cloudwatch_elk.groovy -e $docker_host -g $logGroupName -n my-service -l 60```

Load the last half an hour of logs from a set of instances tagged with name 'my-service':

```groovy cloudwatch_elk.groovy -e $docker_host -g $logGroupName -n my-service -i $instance1,$instance2,$instance3 -l 30```

Load the logs between 2 specific date times from all the instances tagged with name 'my-service':

```groovy cloudwatch_elk.groovy -e $docker_host -g $logGroupName -n my-service -f '27/09/2016 08:00' -t '27/09/2016 09:00'```

## Analyse your logs

Setup Kibana to analyse your logs:

* Access Kibana
* Select ```Settings``` -> ```Indices``` -> ```Configure an index pattern```
* Set the ec2 name used to extract the logs in the ```Index name or pattern``` field
* Kibana will fetch the index mapping and propose ```timestamp``` as Time-field name
* Click on Create and ... you are good to go!

Click on Discover and start analysing your logs.

## Destroy the ELK stack

Logs are persisted in the ELK container. When you are done with your analysis execute ```docker-compose down``` to destroy the
container.



 
 
 
 