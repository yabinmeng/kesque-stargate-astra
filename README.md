# Introduction

In this repo, I'm going to demonstrate how to integrate [Kesque cloud](https://kesque.com/) ([Apache Pulsar](https://pulsar.apache.org/) as a service) with [DataStax Astra](https://astra.datastax.com/) ([Apache Cassandra (C*)](https://cassandra.apache.org/) as a service) through [Stargate](https://stargate.io/) data gateway. 

In particular, I'm going to demonstrate the following functionalities:

* Demonstrate Pulsar producer API to publish messages to Kesque cloud
* Demonstrate Pulsar consumer API to consume messages from Kesque cloud 
* Demonstrate Pulsar Schema registry via Apache AvroSchema that both the Pulsar producer and consumer need to follow
* Demonstrate Stargate Rest API to write the received message into Astra, following the target C* schema
* Demonstrate Stargate GraphQL API to write the received message into Astra, following the target C* schema

## Prerequisite

The test program in this repo is written in Golang and was tested based on version **1.15.5**.

Please also register in advance a free-tier service for [Kesque cloud](https://kesque.com/) and [DataStax Astra](https://astra.datastax.com/).

# Program Overview

## Connection Configuration Properties

Once registered the Kesque cloud and DataStax Astra services, get corresponding connection information from your accounts that allows the program to connect to the Pulsar cluster and the Astra database properly. Put the connection info in the file of **config.properties**

```
[Pulsar]
pulsar_token = <pulsar_cluster_authentication_token>
pulsar_svc_uri  = <pulsar_cluster_service_uri>
pulsar_trust_cert = <pulsar_cluster_connection_certificate_file_name>
pulsar_topicName = <pulsar_topic_name>
pulsar_subscriptionName = <pulsar_subscription_name>

[Astra Stargate]
astra_db_id = <astr_database_id>
astra_region = <astra_database_region>
astra_username = <astra_database_user>
astra_password = <astra_database_password>
astra_keyspace = <astra_database_keyspace>
astra_table = <astra_database_table>
```

## Schema

### Message Schema

The example program is demonstrating a typica **IoT** use case where sensor reading data is collected and streamed through a messaging/streaming platform and then lands into C*. The messages that flow through the platform is following a specific schema, as below (in Apache Avro format):

```
var SensorDataAvroSchema string = `{
	"type": "record",
	"name": "SensorData",
	"namespace": "TestNS",
	"fields" : [
		{"name": "SensorID", "type": "string", "logicalType": "UUID"},
		{"name": "ReadingTime", "type": "string", "logicalType": "time-micros"},
		{"name": "SensorType", "type": "string"},
		{"name": "ReadingValue", "type": "float"}
	]
}`
```

### C* Schema

Correspondingly, the C* table schema looks like below. Compared with the raw message schema, there is an extra column "**dayofyear**" as part of the partition key. This column is added to avoid the potential wide partition issue in such a use case.

**NOTE**: The C* schema (keysapce and table) needs to be created in advance in Astra.

```
CREATE TABLE testks.sensor_data (
    sensorid uuid,
    dayofyear int,
    readingtime timestamp,
    readingvalue float,
    sensortype text static,
    PRIMARY KEY ((sensorid, dayofyear), readingtime)
)
```

## Running the Programs

There are 3 main programs in this repo:

* **producer.go**: publish 10 messages that follow a certain schema (as discussed above) to the Pulsar cluster
* **astraConsumerRest.go**: consume the published messages from the Pulsar cluster and write them into the Astra database, using **Stargate Rest** API
* **astraConsumerGraphQL.go**: consume the published messages from the Pulsar cluster and write them into the Astra database, using **Stargate GraphQL** API