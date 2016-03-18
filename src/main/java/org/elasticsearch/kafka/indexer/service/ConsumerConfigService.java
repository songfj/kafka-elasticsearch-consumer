package org.elasticsearch.kafka.indexer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Created by dhyan on 1/28/16.
 */
@Service
public  class ConsumerConfigService {
    private  static final Logger logger = LoggerFactory.getLogger(ConsumerConfigService.class);
    private  final int kafkaFetchSizeBytesDefault = 10 * 1024 * 1024;

    // Kafka ZooKeeper's IP Address/HostName : port list
    @Value("${kafkaZookeeperList:localhost:2181}")
    private   String kafkaZookeeperList;
    // Zookeeper session timeout in MS
    @Value("${zkSessionTimeoutMs:1000}")
    private   int zkSessionTimeoutMs;
    // Zookeeper connection timeout in MS
    @Value("${zkConnectionTimeoutMs:15000}")
    private   int zkConnectionTimeoutMs;
    // Zookeeper number of retries when creating a curator client
    @Value("${zkCuratorRetryTimes:3}")
    private   int zkCuratorRetryTimes;
    // Zookeeper: time in ms between re-tries when creating a Curator
    @Value("${zkCuratorRetryDelayMs:2000}")
    private   int zkCuratorRetryDelayMs;
    @Value("${kafkaBrokersList:localhost:9092}")
    private   String kafkaBrokersList;
    // Kafka Topic to process messages from
    @Value("${topic:my_log_topic}")
    private   String topic;
    // the below two parameters define the range of partitions to be processed by this app
    // first partition in the Kafka Topic from which the messages have to be processed
    @Value("${firstPartition:0}")
    private   short firstPartition;
    // last partition in the Kafka Topic from which the messages have to be processed
    @Value("${lastPartition:3}")
    private   short lastPartition;
    // Option from where the message fetching should happen in Kafka
    // Values can be: CUSTOM/EARLIEST/LATEST/RESTART.
    // If 'CUSTOM' is set, then 'startOffset' has to be set as an int value
    @Value("${startOffsetFrom:RESTART}")
    private  String startOffsetFrom;
    // int value of the offset from where the message processing should happen
    @Value("${startOffset:0}")
    private   int startOffset;
    // Name of the Kafka Consumer Group
    @Value("${consumerGroupName:kafka_es_indexer}")
    private   String consumerGroupName;
    // SimpleConsumer socket bufferSize
    @Value("${kafkaSimpleConsumerBufferSizeBytes:31457280}")
    private   int kafkaSimpleConsumerBufferSizeBytes;
    // SimpleConsumer socket timeout in MS
    @Value("${kafkaSimpleConsumerSocketTimeoutMs:1000}")
    private   int kafkaSimpleConsumerSocketTimeoutMs;
    // FetchRequest's minBytes value
    @Value("${kafkaFetchSizeMinBytes:31457280}")
    private   int kafkaFetchSizeMinBytes;
    // Preferred Message Encoding to process the message before posting it to ElasticSearch
    @Value("${messageEncoding:UTF-8}")
    private   String messageEncoding;
    // Name of the ElasticSearch Cluster
    @Value("${esClusterName:KafkaESCluster}")
    private   String esClusterName;
    // Name of the ElasticSearch Host Port List
    @Value("${esHostPortList:localhost:9300}")
    private   String esHostPortList;
    // IndexName in ElasticSearch to which the processed Message has to be posted
    @Value("${esIndex:kafkaESIndex}")
    private   String esIndex;
    // IndexType in ElasticSearch to which the processed Message has to be posted
    @Value("${esIndexType:kafkaESType}")
    private   String esIndexType;
    // flag to enable/disable performance metrics reporting
    @Value("${isPerfReportingEnabled:false}")
    private   boolean isPerfReportingEnabled;
    // number of times to try to re-init Kafka connections/consumer if read/write to Kafka fails
    @Value("${numberOfReinitTries:2}")
    private   int numberOfReinitAttempts;
    // sleep time in ms between Kafka re-init atempts
    @Value("${kafkaReinitSleepTimeMs:10000}")
    private   int kafkaReinitSleepTimeMs;
    // sleep time in ms between attempts to index data into ES again
    @Value("${esIndexingRetrySleepTimeMs:10000}")
    private   int esIndexingRetrySleepTimeMs;
    // number of times to try to index data into ES if ES cluster is not reachable
    @Value("${numberOfEsIndexingRetryAttempts:2}")
    private   int numberOfEsIndexingRetryAttempts;

    // Log property file for the consumer instance
    @Value("${logPropertyFile:log4j.properties}")
    private   String logPropertyFile;

    // determines whether the consumer will post to ElasticSearch or not:
    // If set to true, the consumer will read events from Kafka and transform them,
    // but will not post to ElasticSearch
    @Value("${isDryRun:false}")
    private   boolean isDryRun;

    // Wait time in seconds between consumer job rounds
    @Value("${consumerSleepBetweenFetchsMs:10}")
    private   int consumerSleepBetweenFetchsMs;

    //wait time before force-stopping Consumer Job
    @Value("${timeLimitToStopConsumerJob:10}")
    private   int timeLimitToStopConsumerJob ;
    //timeout in seconds before force-stopping Indexer app and all indexer jobs
    @Value("${appStopTimeoutSeconds:10}")
    private   int appStopTimeoutSeconds;

    public int getKafkaFetchSizeBytesDefault() {
        return kafkaFetchSizeBytesDefault;
    }

    public String getKafkaZookeeperList() {
        return kafkaZookeeperList;
    }

    public void setKafkaZookeeperList(String kafkaZookeeperList) {
        this.kafkaZookeeperList = kafkaZookeeperList;
    }

    public int getZkSessionTimeoutMs() {
        return zkSessionTimeoutMs;
    }

    public void setZkSessionTimeoutMs(int zkSessionTimeoutMs) {
        this.zkSessionTimeoutMs = zkSessionTimeoutMs;
    }

    public int getZkConnectionTimeoutMs() {
        return zkConnectionTimeoutMs;
    }

    public void setZkConnectionTimeoutMs(int zkConnectionTimeoutMs) {
        this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
    }

    public int getZkCuratorRetryTimes() {
        return zkCuratorRetryTimes;
    }

    public void setZkCuratorRetryTimes(int zkCuratorRetryTimes) {
        this.zkCuratorRetryTimes = zkCuratorRetryTimes;
    }

    public int getZkCuratorRetryDelayMs() {
        return zkCuratorRetryDelayMs;
    }

    public void setZkCuratorRetryDelayMs(int zkCuratorRetryDelayMs) {
        this.zkCuratorRetryDelayMs = zkCuratorRetryDelayMs;
    }

    public String getKafkaBrokersList() {
        return kafkaBrokersList;
    }

    public void setKafkaBrokersList(String kafkaBrokersList) {
        this.kafkaBrokersList = kafkaBrokersList;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public short getFirstPartition() {
        return firstPartition;
    }

    public void setFirstPartition(short firstPartition) {
        this.firstPartition = firstPartition;
    }

    public short getLastPartition() {
        return lastPartition;
    }

    public void setLastPartition(short lastPartition) {
        this.lastPartition = lastPartition;
    }

    public String getStartOffsetFrom() {
        return startOffsetFrom;
    }

    public void setStartOffsetFrom(String startOffsetFrom) {
        this.startOffsetFrom = startOffsetFrom;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(int startOffset) {
        this.startOffset = startOffset;
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    public void setConsumerGroupName(String consumerGroupName) {
        this.consumerGroupName = consumerGroupName;
    }

    public int getKafkaSimpleConsumerBufferSizeBytes() {
        return kafkaSimpleConsumerBufferSizeBytes;
    }

    public void setKafkaSimpleConsumerBufferSizeBytes(int kafkaSimpleConsumerBufferSizeBytes) {
        this.kafkaSimpleConsumerBufferSizeBytes = kafkaSimpleConsumerBufferSizeBytes;
    }

    public int getKafkaSimpleConsumerSocketTimeoutMs() {
        return kafkaSimpleConsumerSocketTimeoutMs;
    }

    public void setKafkaSimpleConsumerSocketTimeoutMs(int kafkaSimpleConsumerSocketTimeoutMs) {
        this.kafkaSimpleConsumerSocketTimeoutMs = kafkaSimpleConsumerSocketTimeoutMs;
    }

    public int getKafkaFetchSizeMinBytes() {
        return kafkaFetchSizeMinBytes;
    }

    public void setKafkaFetchSizeMinBytes(int kafkaFetchSizeMinBytes) {
        this.kafkaFetchSizeMinBytes = kafkaFetchSizeMinBytes;
    }

    public String getMessageEncoding() {
        return messageEncoding;
    }

    public void setMessageEncoding(String messageEncoding) {
        this.messageEncoding = messageEncoding;
    }

    public String getEsClusterName() {
        return esClusterName;
    }

    public void setEsClusterName(String esClusterName) {
        this.esClusterName = esClusterName;
    }

    public String getEsHostPortList() {
        return esHostPortList;
    }

    public void setEsHostPortList(String esHostPortList) {
        this.esHostPortList = esHostPortList;
    }

    public String getEsIndex() {
        return esIndex;
    }

    public void setEsIndex(String esIndex) {
        this.esIndex = esIndex;
    }

    public String getEsIndexType() {
        return esIndexType;
    }

    public void setEsIndexType(String esIndexType) {
        this.esIndexType = esIndexType;
    }

    public boolean isPerfReportingEnabled() {
        return isPerfReportingEnabled;
    }

    public void setPerfReportingEnabled(boolean perfReportingEnabled) {
        isPerfReportingEnabled = perfReportingEnabled;
    }

    public int getNumberOfReinitAttempts() {
        return numberOfReinitAttempts;
    }

    public void setNumberOfReinitAttempts(int numberOfReinitAttempts) {
        this.numberOfReinitAttempts = numberOfReinitAttempts;
    }

    public int getKafkaReinitSleepTimeMs() {
        return kafkaReinitSleepTimeMs;
    }

    public void setKafkaReinitSleepTimeMs(int kafkaReinitSleepTimeMs) {
        this.kafkaReinitSleepTimeMs = kafkaReinitSleepTimeMs;
    }

    public int getEsIndexingRetrySleepTimeMs() {
        return esIndexingRetrySleepTimeMs;
    }

    public void setEsIndexingRetrySleepTimeMs(int esIndexingRetrySleepTimeMs) {
        this.esIndexingRetrySleepTimeMs = esIndexingRetrySleepTimeMs;
    }

    public int getNumberOfEsIndexingRetryAttempts() {
        return numberOfEsIndexingRetryAttempts;
    }

    public void setNumberOfEsIndexingRetryAttempts(int numberOfEsIndexingRetryAttempts) {
        this.numberOfEsIndexingRetryAttempts = numberOfEsIndexingRetryAttempts;
    }

    public String getLogPropertyFile() {
        return logPropertyFile;
    }

    public void setLogPropertyFile(String logPropertyFile) {
        this.logPropertyFile = logPropertyFile;
    }

    public boolean isDryRun() {
        return isDryRun;
    }

    public void setIsDryRun(boolean isDryRun) {
        this.isDryRun = isDryRun;
    }

    public int getConsumerSleepBetweenFetchsMs() {
        return consumerSleepBetweenFetchsMs;
    }

    public void setConsumerSleepBetweenFetchsMs(int consumerSleepBetweenFetchsMs) {
        this.consumerSleepBetweenFetchsMs = consumerSleepBetweenFetchsMs;
    }

    public int getTimeLimitToStopConsumerJob() {
        return timeLimitToStopConsumerJob;
    }

    public void setTimeLimitToStopConsumerJob(int timeLimitToStopConsumerJob) {
        this.timeLimitToStopConsumerJob = timeLimitToStopConsumerJob;
    }

    public int getAppStopTimeoutSeconds() {
        return appStopTimeoutSeconds;
    }

    public void setAppStopTimeoutSeconds(int appStopTimeoutSeconds) {
        this.appStopTimeoutSeconds = appStopTimeoutSeconds;
    }
}
