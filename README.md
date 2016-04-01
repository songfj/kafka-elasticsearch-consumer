[![Build Status](https://travis-ci.org/BigDataDevs/kafka-elasticsearch-consumer.svg?branch=master)](https://travis-ci.org/BigDataDevs/kafka-elasticsearch-consumer)
<a href='https://bintray.com/bigdatadevs/bigdatadevs-repo/kafka-elasticsearch-consumer/view?source=watch' alt='Get automatic notifications about new "kafka-elasticsearch-consumer" versions'><img src='https://www.bintray.com/docs/images/bintray_badge_color.png'></a>

# Welcome to the kafka-elasticsearch-standalone-consumer wiki!

## Architecture of the kafka-elasticsearch-standalone-consumer [indexer]

![](https://raw.githubusercontent.com/ppine7/kafka-elasticsearch-standalone-consumer/master/img/IndexerV2Design.jpg)


# Introduction

### **Kafka Standalone Consumer [Indexer] will read messages from Kafka, in batches, process and bulk-index them into ElasticSearch.**

### _As described in the illustration above, here is how the indexer works:_

* Kafka has a topic named, say `Topic1`

* Lets say, `Topic1` has 5 partitions.

* In the configuration file, kafka-es-indexer.properties, set firstPartition=0 and lastPartition=4 properties 

* start the indexer application as described below 

* there will be 5 threads started, one for each consumer from each of the partitions

* when a new partition is added to the kafka topic - configuration has to be updated and the indexer application has to be restarted


# How to use ? 

### Running via Gradle 

**1. Download the code into a `$INDEXER_HOME` dir.

**2. cp `$INDEXER_HOME`/src/main/resources/config/kafka-es-indexer.properties /your/absolute/path/kafka-es-indexer.properties file - update all relevant properties as explained in the comments

**3. cp `$INDEXER_HOME`/src/main/resources/config/logback.xml /your/absolute/path/logback.xml

 specify directory you want to store logs in:
	<property name="LOG_DIR" value="/tmp"/>
	
 adjust values of max sizes and number of log files as needed

**4. modify $INDEXER_HOME`/src/main/resources/spring/kafka-es-contect.xml if needed

	If you want to use custom IMessageHandler and/or IIndexHandler classes - specify them in the following config:
	(make sure to only modify the class name, not the beans' name/scope)
	
	<bean id="messageHandler" 
		class="org.elasticsearch.kafka.indexer.service.impl.BasicMessageHandler"
		scope="prototype"/>
    <bean id="indexHandler" class="org.elasticsearch.kafka.indexer.service.impl.BasicIndexHandler"/>
	
		
**5. build the app:

	cd $INDEXER_HOME
    ./gradlew clean jar
     	
 The kafka-es-indexer-2.0.jar will be created in the $INDEXER_HOME/build/libs/ dir.

**5. make sure your $JAVA_HOME env variable is set (use JDK1.8 or above);
	you may want to adjust JVM options and other values in the gradlew script and gradle.properties file

**6. run the app:

	./gradlew run -Dindexer.properties=/your/absolute/path//kafka-es-indexer.properties -Dlogback.configurationFile=/your/absolute/path/logback.xml
 
 	OR, if you want to run with the default properties and logback files packaged in the /src/main/resources/config/ dir:
	./gradlew run
	
# Versions

* Kafka Version: 0.8.2.1

* ElasticSearch: 1.5.x -1.6.x (2.0 is in the development)

* Scala Version for Kafka Build: 2.10.0

* JDK 1.8

# Configuration

Indexer application properties are specified in the kafka_es_indexer.properties file:

[kafka-es-indexer.properties](https://github.com/BigDataDevs/kafka-elasticsearch-consumer/blob/master/src/main/resources/config/kafka-es-indexer.properties)

Logging properties are specified in the logback.xml file: 
[logback.xml](https://github.com/BigDataDevs/kafka-elasticsearch-consumer/blob/master/src/main/resources/config/logback.xml)

Indexer application Spring configuration is specified in the kafka-es-context.xml:
[kafka-es-context.xml](https://github.com/BigDataDevs/kafka-elasticsearch-consumer/blob/master/src/main/resources/spring/kafka-es-context.xml)

# Customization

Indexer applicatin can be easily customized. Two main areas for customizations are: 
* message handling/conversion
	examples of use cases for this customization:
	-- your incoming messages are not in a JSON format compatible with the expected ES message formats
	-- your messages have to be enreached with data from other sources (via other meta-data lookups, etc.)
	-- you want to selectively index messages into ES based on some custom criteria
	
* ES index management
	examples of use cases for this customization:
	-- you have to index messages into different ES indexes based on custom logic/ properties of the messages
	-- you have to determine index type based on custom logic and/or mesage properties

## Message handling customization options

Message handling can be customized by using IMessageHandler interface and/or BasicMessageHandler implementation classes:

* `org.elasticsearch.kafka.indexer.service.IMessageHandler` is an interface that defines main methods for reading events from Kafka, processing them, and bulk-intexing into ElasticSearch. One can implement all or some of the methods if custom behavior is needed. In most cases, the only method you would want to customize would be the `transformMessage(...)` method

* `org.elasticsearch.kafka.indexer.service.impl.BasicMessageHandler` is a basic implementation of the IMessageHandler interface. It does not modify incoming messages in any way and batch-indexes them into ES as is, in the 'UTF-8' format. 


 There are two main approaches to do the customization:
  
-- Approach #1: implement the IMessageHandler interface, inject the BasicMessageHandler into your implementation class and delegate most of the methods to the BasicMessageHandler class, overwriting only the transformMessage() method (and maybe others on very rare occasions). See `org.elasticsearch.kafka.indexer.examples.SimpleMessageHandlerImpl` for an example of such customization. While this approach a slightly more complex than the second one, it provides you with an easy way to mock some or all services while unit testing your custom logic. 

-- Approach #2: extend the BasicMessageHandler class and overwrite transformMessage() method (and others if needed). See `org.elasticsearch.kafka.indexer.examples.RawLogMessageHandlerImpl` for an example of such customization. This is a simpler to implement approach, but is less flexible for Unit testing, since the constructor of the BasicMessageHandler will be called early on.


* _**Do remember to specify your custom message handler class in the kafka-es-context.xml file. By default, BasicMessageHandler will be used**_

## ES index management customization 
Index and index type management/determination customization can be done by using the IIndexHandler interface and/or BasicIdexManager implementation classes:

* `org.elasticsearch.kafka.indexer.service.IIndexHandler` is an interface that defines two methods: getIndexName(params) and getIndexType(params). You can customize both or either of them as needed 

* `org.elasticsearch.kafka.indexer.service.impl.BasicIndexHandler` is a simple imlementation of this interface that returnes indexName and indexType values as configured in the kafka-es-indexer.properties file. 

* you can either implement the interface or extend the basic impl class - either approach is simple enough

* _**Do remember to specify your custom index handler class in the kafka-es-context.xml file. By default, BasicIndexHandler is used**_

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
