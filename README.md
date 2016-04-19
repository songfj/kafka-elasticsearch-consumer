[![Build Status](https://travis-ci.org/BigDataDevs/kafka-elasticsearch-consumer.svg?branch=master)](https://travis-ci.org/BigDataDevs/kafka-elasticsearch-consumer)
[ ![Download](https://api.bintray.com/packages/bigdatadevs/bigdatadevs-repo/kafka-elasticsearch-consumer/images/download.svg) ](https://bintray.com/bigdatadevs/bigdatadevs-repo/kafka-elasticsearch-consumer/_latestVersion)

# Welcome to the kafka-elasticsearch-standalone-consumer wiki!

## Architecture of the kafka-elasticsearch-standalone-consumer [indexer]

![](https://github.com/BigDataDevs/kafka-elasticsearch-consumer/tree/master/img/IndexerV2Design.jpg)


# Introduction

### **Kafka Standalone Consumer [Indexer] will read messages from Kafka, in batches, process and bulk-index them into ElasticSearch.**

### _As described in the illustration above, here is how the indexer works:_

* Lets assume Kafka has a topic `Topic1` with 5 partitions

* In the configuration file, kafka-es-indexer.properties, set firstPartition=0 and lastPartition=4 properties 

* start the indexer application as described below 

* there will be 5 threads started, one for each consumer for each of the partitions

* each job will be reading events from Kafka and indexing them (in batches) into ElasticSearch - using configured index name and type

* when a new partition is added to the kafka topic - configuration has to be updated and the indexer application has to be restarted


# How to use ? 

### Running via Gradle 

1. Download the code into a `$INDEXER_HOME` dir.

2. cp `$INDEXER_HOME`/src/main/resources/config/kafka-es-indexer.properties /your/absolute/path/kafka-es-indexer.properties.template file - update all relevant properties as explained in the comments

3. cp `$INDEXER_HOME`/src/main/resources/config/logback.xml.template /your/absolute/path/logback.xml

 specify directory you want to store logs in:
	<property name="LOG_DIR" value="/tmp"/>
	
 adjust values of max sizes and number of log files as needed

4. modify $INDEXER_HOME`/src/main/resources/spring/kafka-es-context-public.xml if needed

	If you want to use custom IMessageHandler class - specify it in the following config:
	(make sure to only modify the class name, not the beans' name/scope)
	
	 <bean id="messageHandler"
          class="org.elasticsearch.kafka.indexer.service.impl.examples.SimpleMessageHandlerImpl"
          scope="prototype"/>
	
		
5. build the app:

 cd $INDEXER_HOME
 ./gradlew clean jar
     	
 The kafka-es-indexer-2.0.jar will be created in the $INDEXER_HOME/build/libs/ dir.

6. make sure your $JAVA_HOME env variable is set (use JDK1.8 or above);
	you may want to adjust JVM options and other values in the gradlew script and gradle.properties file

7. run the app:

	./gradlew run -Dindexer.properties=/your/absolute/path/kafka-es-indexer.properties -Dlogback.configurationFile=/your/absolute/path/logback.xml
 
### Running via generated scripts:

* Steps 1 - 6 are the same
* run:  ./gradlew clean installDist

* cd ../build/install/kafka-elasticsearch-consumer/bin dir:
![](https://github.com/BigDataDevs/kafka-elasticsearch-consumer/tree/master/img/build-dir.png)

* run `./kafka-elasticsearch-consumer -Dindexer.properties=/your/absolute/path/kafka-es-indexer.properties -Dlogback.configurationFile=/your/absolute/path/logback.xml` script


# Versions

* Kafka Version: 0.8.2.1

* ElasticSearch: 1.5.x -1.6.x (2.0 is in the development)

* Scala Version for Kafka Build: 2.10.0

* JDK 1.8

# Configuration

Indexer application properties are specified in the kafka_es_indexer.properties.template file - you have to create your own copy of this file and adjust properties for your env:
[kafka-es-indexer.properties.template](https://github.com/BigDataDevs/kafka-elasticsearch-consumer/blob/master/src/main/resources/config/kafka-es-indexer.properties.template)
Example of customized properties:
[kafka-es-indexer-local.properties](https://github.com/BigDataDevs/kafka-elasticsearch-consumer/blob/master/src/test/resources/config/kafka-es-indexer-local.properties)
You specify your own properties file via `-Dindexer.properties=/abs-path/your.properties` property


Logging properties are specified in the logback.xml.template file - you have to create your own copy of this file and adjust properties for your env:
[logback.xml.template](https://github.com/BigDataDevs/kafka-elasticsearch-consumer/blob/master/src/main/resources/config/logback.xml.template)
Example logback-test.xml is here: 
[logback-test.xml](https://github.com/BigDataDevs/kafka-elasticsearch-consumer/blob/master/src/test/resources/config/logback-test.xml)
You specify your own logback config file via `-Dlogback.configurationFile=/abs-path/your-logback.xml` property  

Indexer application Spring configuration is specified in the kafka-es-context-public.xml:
[kafka-es-context.xml](https://github.com/BigDataDevs/kafka-elasticsearch-consumer/blob/master/src/main/resources/spring/kafka-es-context-public.xml)

# Customization

Indexer applicatin can be easily customized. The main areas for customizations are: 
* message handling/conversion
	examples of use cases for this customization:
	-- your incoming messages are not in a JSON format compatible with the expected ES message formats
	-- your messages have to be enreached with data from other sources (via other meta-data lookups, etc.)
	-- you want to selectively index messages into ES based on some custom criteria
* index name/type customization
	
## ES message handling customization 
Message handling can be customized by implementing the IMessageHandler interface :

* `org.elasticsearch.kafka.indexer.service.IMessageHandler` is an interface that defines main methods for reading events from Kafka, processing them, and bulk-intexing into ElasticSearch. One can implement all or some of the methods if custom behavior is needed. You can customize:
* `transformMessage(...)` method to transform an event from one format into another;
* `addEventToBatch(...)` method - adding an event to specified (or custom ) index, with or without routing info
* `postToElasticSearch(...)` method - most likely you won't need to customize this

To do this customization,  you implement the IMessageHandler interface and inject the `ElasticSearchBatchService` into your implementation class and delegate most of the methods to the ElasticSearchBatchService class. ElasticSearchBatchService gives you basic batching operations.

See `org.elasticsearch.kafka.indexer.service.impl.examples.SimpleMessageHandlerImpl` for an example of such customization. 

* _**Do remember to specify your custom message handler class in the kafka-es-context-public.xml file. By default, SimpleMessageHandlerImpl will be used**_

## ES index name/type management customization 
Index name and index type management/determination customization can be done by providing custom logic in your implementation of the IMessageHandler interface:

* `org.elasticsearch.kafka.indexer.service.impl.examples.SimpleMessageHandlerImpl`  uses  indexName and indexType values as configured in the kafka-es-indexer.properties file. If you want to use custom logic - add it to the `addEventToBatch(...)` method

# Running as a Docker Container

TODO

# License

kafka-elasticsearch-standalone-consumer

	Licensed under the Apache License, Version 2.0 (the "License"); you may
	not use this file except in compliance with the License. You may obtain
	a copy of the License at

	     http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing,
	software distributed under the License is distributed on an
	"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
	KIND, either express or implied.  See the License for the
	specific language governing permissions and limitations
	under the License.

# Contributors

 - [Krishna Raj](https://github.com/reachkrishnaraj)
 - [Marina Popova](https://github.com/ppine7)
 - [Dhyan Muralidharan](https://github.com/dhyan-yottaa)
