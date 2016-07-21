package org.elasticsearch.kafka.indexer.service;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import org.elasticsearch.kafka.indexer.exception.KafkaClientNotRecoverableException;
import org.elasticsearch.kafka.indexer.exception.KafkaClientRecoverableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaClientService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaClientService.class);
    @Value("${topic:my_log_topic}")
    private String topic;
    // Kafka ZooKeeper's IP Address/HostName : port list
    //@Value("${kafkaZookeeperList:localhost:2181}")
    //private String kafkaZookeeperList;
    // Zookeeper session timeout in MS
    //@Value("${zkSessionTimeoutMs:1000}")
    //private int zkSessionTimeoutMs;
    // Zookeeper connection timeout in MS
    //@Value("${zkConnectionTimeoutMs:15000}")
    //private int zkConnectionTimeoutMs;
    // Zookeeper number of retries when creating a curator client
    //@Value("${zkCuratorRetryTimes:3}")
    //private int zkCuratorRetryTimes;
    // Zookeeper: time in ms between re-tries when creating a Curator
    //@Value("${zkCuratorRetryDelayMs:2000}")
    //private int zkCuratorRetryDelayMs;
    @Value("${kafkaBrokersList:localhost:9092}")
    private String kafkaBrokersList;
    private String[] kafkaBrokersArray;
    @Value("${kafkaReinitSleepTimeMs:10000}")
    private int kafkaReinitSleepTimeMs;
    @Value("${numberOfReinitTries:2}")
    private int numOfReinitAttempts;
    // the below two parameters define the range of partitions to be processed by this app
    // first partition in the Kafka Topic from which the messages have to be processed
    @Value("${firstPartition:0}")
    private short firstPartition;
    // last partition in the Kafka Topic from which the messages have to be processed
    @Value("${lastPartition:3}")
    private short lastPartition;
    // Option from where the message fetching should happen in Kafka
    // Values can be: CUSTOM/EARLIEST/LATEST/RESTART.
    // If 'CUSTOM' is set, then 'startOffset' has to be set as an int value
    @Value("${startOffsetFrom:RESTART}")
    private String startOffsetFrom;
    // int value of the offset from where the message processing should happen
    @Value("${startOffset:0}")
    private int startOffset;
    // Name of the Kafka Consumer Group
    @Value("${consumerGroupName:kafka_es_indexer}")
    private String consumerGroupName;
    // SimpleConsumer socket bufferSize
    @Value("${kafkaSimpleConsumerBufferSizeBytes:31457280}")
    private int kafkaSimpleConsumerBufferSizeBytes;
    // SimpleConsumer socket timeout in MS
    @Value("${kafkaSimpleConsumerSocketTimeoutMs:1000}")
    private int kafkaSimpleConsumerSocketTimeoutMs;
    // FetchRequest's minBytes value
    @Value("${kafkaFetchSizeMinBytes:31457280}")
    private int kafkaFetchSizeMinBytes;
    @Value("${leaderFindRetryCount:5}")
    private int leaderFindRetryCount ;

    private SimpleConsumer simpleConsumer;
    private String kafkaClientId;
    private  int partition =-1;
    private String leaderBrokerHost;
    private int leaderBrokerPort;
    private String leaderBrokerURL;

    public KafkaClientService(int partition)  {
        this.partition = partition;
    }
    public KafkaClientService()  {}

    @PostConstruct
    public void init() throws Exception {
        if(partition < 0){
            throw new Exception("Partition id not assigned");
        }
        logger.info("Initializing KafkaClient for topic={}, partition={} ", topic, partition);
        kafkaClientId = consumerGroupName + "_" + partition;
        kafkaBrokersArray = kafkaBrokersList.trim().split(",");
        logger.info("### KafkaClient Config: ###");
        //logger.info("kafkaZookeeperList: {}", kafkaZookeeperList);
        logger.info("kafkaBrokersList: {}", kafkaBrokersList);
        logger.info("kafkaClientId: {}", kafkaClientId);
        logger.info("topic: {}", topic);
        logger.info("partition: {}", partition);
        findLeader();
        initConsumer();
    }


    public void reInitKafka() throws Exception {
        for (int i = 0; i < numOfReinitAttempts; i++) {
            try {
                logger.info("Re-initializing KafkaClient for partition {}, try # {}", partition, i);
                close();
                logger.info("Kafka client closed for partition {}. Will sleep for {} ms to allow kafka to stabilize",
                        partition, kafkaReinitSleepTimeMs);
                Thread.sleep(kafkaReinitSleepTimeMs);
                logger.info("Connecting to zookeeper again for partition {}", partition);
                findLeader();
                initConsumer();
                logger.info(".. trying to get offsets for partition={} ... ", partition);
                checkKafkaOffsets();
                logger.info("KafkaClient re-intialization for partition={} finished OK", partition);
                return;
            } catch (Exception e) {
                if (i < numOfReinitAttempts) {
                    logger.info("Re-initializing KafkaClient for partition={}, try#={} - still failing with Exception: {}",
                            partition, i, e.getMessage());
                } else {
                    // if we got here - we failed to re-init Kafka after the max # of attempts - throw an exception out
                    logger.error("KafkaClient re-initialization failed for partition={} after {} attempts - throwing exception: "
                            + e.getMessage(), partition, numOfReinitAttempts);
                    throw new KafkaClientRecoverableException("KafkaClient re-initialization failed for partition=" + partition +
                            " after " + numOfReinitAttempts + " attempts - throwing exception: "
                            + e.getMessage(), e);
                }
            }
        }
    }
    public void initConsumer() throws Exception {
        try {
            simpleConsumer = new SimpleConsumer(
                    leaderBrokerHost, leaderBrokerPort, kafkaSimpleConsumerSocketTimeoutMs,
                    kafkaSimpleConsumerBufferSizeBytes, kafkaClientId);
            logger.info("Initialized Kafka Consumer successfully for partition {}", partition);
        } catch (Exception e) {
            logger.error("Failed to initialize Kafka Consumer: " + e.getMessage());
            throw e;
        }
    }

    public void checkKafkaOffsets() throws Exception {
        try {
            long currentOffset = fetchCurrentOffsetFromKafka();
            long earliestOffset = getEarliestOffset();
            long latestOffset = getLastestOffset();
            logger.info("Offsets for partition={}: currentOffset={}; earliestOffset={}; latestOffset={}",
                    partition, currentOffset, earliestOffset, latestOffset);
        } catch (Exception e) {
            logger.error("Exception from checkKafkaOffsets(): for partition={}", partition, e);
            throw e;
        }

    }

    public void saveOffsetInKafka(long offset, short errorCode) throws KafkaClientRecoverableException {
        logger.debug("Starting to save the Offset value to Kafka: offset={}, errorCode={} for partition {}",
                offset, errorCode, partition);
        short versionID = 0;
        int correlationId = 0;
        try {
            TopicAndPartition tp = new TopicAndPartition(topic, partition);
            OffsetAndMetadata offsetMetaAndErr = new OffsetAndMetadata(
                    offset, OffsetAndMetadata.NoMetadata(), errorCode);
            Map<TopicAndPartition, OffsetAndMetadata> mapForCommitOffset = new HashMap<>();
            mapForCommitOffset.put(tp, offsetMetaAndErr);
            kafka.javaapi.OffsetCommitRequest offsetCommitReq = new kafka.javaapi.OffsetCommitRequest(
                    kafkaClientId, mapForCommitOffset, correlationId, kafkaClientId, versionID);
            OffsetCommitResponse offsetCommitResp = simpleConsumer.commitOffsets(offsetCommitReq);
            logger.debug("Saved offset={} for partition={}; OffsetCommitResponse ErrorCode = {}",
                    offset, partition, offsetCommitResp.errors().get(tp));
            Short responseErrorCode = (Short) offsetCommitResp.errors().get(tp);
            if (responseErrorCode != null && responseErrorCode != ErrorMapping.NoError()) {
                // TODO 4: verify this is the only condition that means that the commit was successful
                throw new KafkaClientRecoverableException("Error saving offset=" + offset + " for partition=" + partition +
                        ": errorCode is not good: " + responseErrorCode);
            }
        } catch (Exception e) {
            logger.error("Error commiting offset={} for partition={}: ", offset, partition, e);
            throw new KafkaClientRecoverableException("Error saving offset=" + offset + " for partition=" +
                    partition + "; error: " + e.getMessage(), e);
        }

    }

    public PartitionMetadata findLeader() throws Exception {
        logger.info("Looking for Kafka leader broker for partition {}...", partition);
        PartitionMetadata leaderPartitionMetaData = findLeaderWithRetry();
        if (leaderPartitionMetaData == null || leaderPartitionMetaData.leader() == null) {
            logger.error("Failed to find leader for topic=[{}], partition=[{}], kafka brokers list: [{}]: PartitionMetadata is null",
                    topic, partition, kafkaBrokersList);
            throw new Exception("Failed to find leader for topic=[" + topic +
                    "], partition=[" + partition +
                    ", kafka brokers list: [" + kafkaBrokersList +
                    "]: currentPartitionMetadata is null");

        }
        leaderBrokerHost = leaderPartitionMetaData.leader().host();
        leaderBrokerPort = leaderPartitionMetaData.leader().port();
        leaderBrokerURL = leaderBrokerHost + ":" + leaderBrokerPort;
        logger.info("Found leader: leaderBrokerURL={}", leaderBrokerURL);
        return leaderPartitionMetaData;
    }

    private PartitionMetadata findLeaderWithRetry() throws Exception{
        PartitionMetadata leaderPartitionMetaData = null ;
        int retryCount =leaderFindRetryCount ;
        while(retryCount >0){
            for (int i=0; i<kafkaBrokersArray.length; i++) {
                String brokerStr = kafkaBrokersArray[i];
                String[] brokerStrTokens = brokerStr.split(":");
                if (brokerStrTokens.length < 2) {
                    logger.error(
                            "Failed to find Kafka leader broker - wrong config, not enough tokens: brokerStr={}",
                            brokerStr);
                    throw new Exception("Failed to find Kafka leader broker - wrong config, not enough tokens: brokerStr=" +
                            brokerStr);
                }
                leaderPartitionMetaData = findLeaderWithBroker(brokerStrTokens[0], brokerStrTokens[1]);
                // we found the leader - no need to query the rest of the brokers, get out of the loop
                if (leaderPartitionMetaData != null)
                    return leaderPartitionMetaData;
            }
            retryCount-- ;
            logger.error("Leader find attempt {} failed .Retrying after sleep",5-retryCount);
            Thread.sleep(1000);
        }
        return leaderPartitionMetaData;
    }

    private PartitionMetadata findLeaderWithBroker(
            String kafkaBrokerHost, String kafkaBrokerPortStr) {
        logger.info("Looking for leader for partition {} using Kafka Broker={}:{}, topic={}",
                partition, kafkaBrokerHost, kafkaBrokerPortStr, topic);
        PartitionMetadata leaderPartitionMetadata = null;
        SimpleConsumer leadFindConsumer = null;
        try {
            int kafkaBrokerPort = Integer.parseInt(kafkaBrokerPortStr);
            leadFindConsumer = new SimpleConsumer(
                    kafkaBrokerHost, kafkaBrokerPort, kafkaSimpleConsumerSocketTimeoutMs,
                    kafkaSimpleConsumerBufferSizeBytes, "leaderLookup");
            List<String> topics = new ArrayList<String>();
            topics.add(this.topic);
            TopicMetadataRequest req = new TopicMetadataRequest(topics);
            kafka.javaapi.TopicMetadataResponse resp = leadFindConsumer.send(req);

            List<TopicMetadata> metaData = resp.topicsMetadata();
            for (TopicMetadata item : metaData) {
                for (PartitionMetadata part : item.partitionsMetadata()) {
                    if (part.partitionId() == partition) {
                        leaderPartitionMetadata = part;
                        logger.info("Found leader for partition {} using Kafka Broker={}:{}, topic={}; leader broker URL: {}:{}",
                                partition, kafkaBrokerHost, kafkaBrokerPortStr, topic,
                                leaderPartitionMetadata.leader().host(), leaderPartitionMetadata.leader().port());
                        break;
                    }
                }
                // we found the leader - get out of this loop as well
                if (leaderPartitionMetadata != null)
                    break;
            }
        } catch (Exception e) {
            logger.warn("Failed to find leader for partition {} using Kafka Broker={} , topic={}; Error: {}",
                    partition, kafkaBrokerHost, topic, e.getMessage());
        } finally {
            if (leadFindConsumer != null) {
                leadFindConsumer.close();
                logger.debug("Closed the leadFindConsumer connection");
            }
        }
        return leaderPartitionMetadata;
    }

    public long getLastestOffset() throws KafkaClientRecoverableException {
        logger.debug("Getting LastestOffset for topic={}, partition={}, kafkaGroupId={}",
                topic, partition, kafkaClientId);
        long latestOffset = getOffset(topic, partition, OffsetRequest.LatestTime(), kafkaClientId);
        logger.debug("LatestOffset={}  for partition {}", latestOffset, partition);
        return latestOffset;
    }

    public long getEarliestOffset() throws KafkaClientRecoverableException {
        logger.debug("Getting EarliestOffset for topic={}, partition={}, kafkaGroupId={}",
                topic, partition, kafkaClientId);
        long earliestOffset = this.getOffset(topic, partition, OffsetRequest.EarliestTime(), kafkaClientId);
        logger.debug("earliestOffset={} for partition {}", earliestOffset, partition);
        return earliestOffset;
    }

    private long getOffset(String topic, int partition, long whichTime, String clientName)
            throws KafkaClientRecoverableException {
        try {
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
            Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
            requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
            kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                    requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
            OffsetResponse response = simpleConsumer.getOffsetsBefore(request);

            if (response.hasError()) {
                // treat this as a recoverable exception - we will try to re-init Kafka
                // TODO: verify if there are some cases when this should not be recoverable
                int errorCode = response.errorCode(topic, partition);
                logger.error("Error fetching offsets from Kafka for partition={}; Reason: {}",
                        partition, errorCode);
                throw new KafkaClientRecoverableException("Error fetching offsets from Kafka. Reason: " +
                        errorCode + " for partition=" + partition);
            }
            long[] offsets = response.offsets(topic, partition);
            return offsets[0];
        } catch (Exception e) {
            logger.error("Exception fetching offsets for partition={}", partition, e);
            throw new KafkaClientRecoverableException("Error fetching offsets from Kafka for partition=" +
                    partition + "; error: " + e.getMessage());
        }
    }

    public long fetchCurrentOffsetFromKafka() throws KafkaClientRecoverableException {
        short versionID = 0;
        int correlationId = 0;
        try {
            List<TopicAndPartition> topicPartitionList = new ArrayList<TopicAndPartition>();
            TopicAndPartition myTopicAndPartition = new TopicAndPartition(topic, partition);
            topicPartitionList.add(myTopicAndPartition);
            OffsetFetchRequest offsetFetchReq = new OffsetFetchRequest(
                    kafkaClientId, topicPartitionList, versionID, correlationId, kafkaClientId);
            OffsetFetchResponse offsetFetchResponse = simpleConsumer.fetchOffsets(offsetFetchReq);
            long currentOffset = offsetFetchResponse.offsets().get(myTopicAndPartition).offset();
            //logger.info("Fetched Kafka's currentOffset = " + currentOffset);
            return currentOffset;
        } catch (Exception e) {
            logger.error("Exception fetching current offset for partition={}", partition, e);
            throw new KafkaClientRecoverableException("Exception fetching current offset for partition=" +
                    partition + "; error: " + e.getMessage(), e);
        }
    }

    public long computeInitialOffset() throws Exception {
        long offsetForThisRound = -1L;
        long earliestOffset = getEarliestOffset();
        // TODO 6: use Enum for startOffsetFrom values
        logger.info("**** Computing Kafka offset *** for partition={}, startOffsetFrom={}, earliestOffset={}",
                partition, startOffsetFrom, earliestOffset);
        if (startOffsetFrom.equalsIgnoreCase("CUSTOM")) {
            long customOffset = startOffset;
            logger.info("Restarting from the CUSTOM offset={} for topic={}, partition={}",
                    customOffset, topic, partition);
            if (customOffset >= 0) {
                offsetForThisRound = customOffset;
            } else {
                throw new KafkaClientNotRecoverableException("Custom offset=" + customOffset +
                        "for topic [" + topic + "], partition [" + partition +
                        "] is < 0, which is not an acceptable value; exiting");
            }
        } else if (startOffsetFrom.equalsIgnoreCase("EARLIEST")) {
            logger.info("Restarting from the EARLIEST offset={} for topic={}, partition={}",
                    earliestOffset, topic, partition);
            offsetForThisRound = getEarliestOffset();
        } else if (startOffsetFrom.equalsIgnoreCase("LATEST")) {
            offsetForThisRound = getLastestOffset();
            logger.info("Restarting from the LATEST offset={} for topic={}, partition={}",
                    offsetForThisRound, topic, partition);
        } else if (startOffsetFrom.equalsIgnoreCase("RESTART")) {
            offsetForThisRound = fetchCurrentOffsetFromKafka();
            logger.info("Restarting from the last committed offset={} for topic={}, partition={}",
                    offsetForThisRound, topic, partition);
        }
        // for any of the cases - make sure the offset in not less than 0:
        if (offsetForThisRound < 0) {
            if (earliestOffset >= 0) {
                // if this is the first time this client tried to read - offset might be -1
                // try to get set the offset to the EARLIEST available - it may lead
                // to processing events that may have already be processed - but it is safer than
                // starting from the Latest offset in case not all events were processed before
                offsetForThisRound = earliestOffset;
                logger.info("offsetForThisRound is set to the Earliest offset since currentOffset is < 0; offsetForThisRound={} for partition {}",
                        offsetForThisRound, partition);
                // also store this as the CurrentOffset to Kafka - to avoid the multiple cycles through
                // this logic in the case no events are coming to the topic for a long time and
                // we always get currentOffset as -1 from Kafka;
                // if there are exceptions during save - they will be thrown out from the saveOffsetInKafka()
                saveOffsetInKafka(offsetForThisRound, ErrorMapping.NoError());
            } else {
                logger.error(
                        "Failed to set the current offset to Earliest offset since he earliest offset is < 0, exiting; partition={}", partition);
                throw new KafkaClientRecoverableException(
                        "Failed to set the current offset to Earliest offset since he earliest offset is < 0, exiting; partition=" + partition);
            }
        }

        // check for a corner case when the computed offset (either current or custom)
        // is less than the Earliest offset - which could happen if some messages were
        // cleaned up from the topic/partition due to retention policy
        if (offsetForThisRound < earliestOffset) {
            logger.warn("WARNING: computed offset={} is less than EarliestOffset = {}" +
                    "; setting offsetForThisRound to the EarliestOffset for partition {}", offsetForThisRound, earliestOffset, partition);
            offsetForThisRound = earliestOffset;
            try {
                saveOffsetInKafka(offsetForThisRound, ErrorMapping.NoError());
            } catch (KafkaClientRecoverableException e) {
                logger.error("Failed to commit offset after resetting it to Earliest={} for partition={}; throwing exception ",
                        offsetForThisRound, partition, e);
                throw e;
            }
        }
        logger.info("Resulting offsetForThisRound={} for partition={}", offsetForThisRound, partition);
        return offsetForThisRound;
    }

    public FetchResponse getMessagesFromKafka(long offset) throws KafkaClientRecoverableException {
        logger.debug("Starting getMessagesFromKafka() ...");
        try {
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(kafkaClientId)
                    .addFetch(topic, partition, offset, kafkaFetchSizeMinBytes)
                    .build();
            FetchResponse fetchResponse = simpleConsumer.fetch(req);
            return fetchResponse;
        } catch (Exception e) {
            logger.error("Exception fetching messages from Kafka for partition {}", partition, e);
            throw new KafkaClientRecoverableException("Exception fetching messages from Kafka for partition=" +
                    partition + "; error: " + e.getMessage(), e);
        }
    }

    // TODO 1: review how we handle each type of errors - some should fail the whole batch, not just ignored or re-tried
    // TODO 2: also make sure we do not re-process the same batch forever: call() -> reInitKafka() -> call() -> ...
    // TODO 3: verify that the only case when we need to roll-back to the Earliest offset is  the OffsetOutOfRange error
    public Long handleErrorFromFetchMessages(short errorCode, long offsetForThisRound) throws Exception {
        // Do things according to the error code
        logger.error("Error fetching events from Kafka, error code={}, partition={}",
                errorCode, partition);
        if (errorCode == ErrorMapping.BrokerNotAvailableCode()) {
            logger.error("BrokerNotAvailableCode error - ReInitiating Kafka Client for partition {}", partition);
            reInitKafka();
        } else if (errorCode == ErrorMapping.InvalidFetchSizeCode()) {
            logger.error("InvalidFetchSizeCode error - ReInitiating Kafka Client for partition {}", partition);
            reInitKafka();
        } else if (errorCode == ErrorMapping.InvalidMessageCode()) {
            logger.error("InvalidMessageCode error - not handling it. Returning for partition {}", partition);
        } else if (errorCode == ErrorMapping.LeaderNotAvailableCode()) {
            logger.error("LeaderNotAvailableCode error - ReInitiating Kafka Client for partition {}", partition);
            reInitKafka();
        } else if (errorCode == ErrorMapping.MessageSizeTooLargeCode()) {
            logger.error("MessageSizeTooLargeCode error - not handling it. Returning for partition {}", partition);
        } else if (errorCode == ErrorMapping.NotLeaderForPartitionCode()) {
            logger.error("NotLeaderForPartitionCode error - ReInitiating Kafka Client for partition {}", partition);
            reInitKafka();
        } else if (errorCode == ErrorMapping.OffsetMetadataTooLargeCode()) {
            logger.error("OffsetMetadataTooLargeCode error - not handling it. Returning for partition {}", partition);
        } else if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
            logger.error("OffsetOutOfRangeCode error: partition={}, offsetForThisRound={}",
                    partition, offsetForThisRound);
            long earliestOffset = getEarliestOffset();
            // The most likely reason for this error is that the consumer is trying to read events from an offset
            // that has already expired from the Kafka topic due to retention period;
            // In that case the only course of action is to start processing events from the EARLIEST available offset;
            // check for a corner case when the EarliestOfset might still be -1
            // in that case - just throw an  exception out and restart indexer after fixing Kafka
            if (earliestOffset < 0) {
                logger.error("OffsetOutOfRangeCode error for partition={} - EARLIEST offset is bad: {}, exiting with an exception",
                        partition, earliestOffset);
                throw new Exception("OffsetOutOfRangeCode error for partition=" + partition +
                        " - EARLIEST offset is bad: " + earliestOffset + ", exiting with an exception");
            }
            logger.info("OffsetOutOfRangeCode error: setting offset for partition {} to the EARLIEST possible offset: {}",
                    partition, earliestOffset);
            try {
                saveOffsetInKafka(earliestOffset, errorCode);
            } catch (Exception e) {
                // throw an exception as this will break reading messages in the next round
                // TODO 5: verify that the IndexerJob is stopped cleanly in this case
                logger.error("Failed to commit offset in Kafka after OffsetOutOfRangeCode - exiting for partition {} ", partition, e);
                throw e;
            }
            return earliestOffset;
        } else if (errorCode == ErrorMapping.ReplicaNotAvailableCode()) {
            logger.error("ReplicaNotAvailableCode error - re-init-ing Kafka for partition {}", partition);
            reInitKafka();
        } else if (errorCode == ErrorMapping.RequestTimedOutCode()) {
            logger.error("RequestTimedOutCode error - re-init-ing Kafka for partition {}", partition);
            reInitKafka();
        } else if (errorCode == ErrorMapping.StaleControllerEpochCode()) {
            logger.error("StaleControllerEpochCode error - not handling it. Returning for partition {}", partition);
            reInitKafka();
        } else if (errorCode == ErrorMapping.StaleLeaderEpochCode()) {
            logger.error("StaleLeaderEpochCode error - not handling it. Returning for partition {}", partition);
            reInitKafka();
        } else if (errorCode == ErrorMapping.UnknownCode()) {
            logger.error("UnknownCode error - re-init-ing Kafka for partition {}", partition);
            reInitKafka();
        } else if (errorCode == ErrorMapping.UnknownTopicOrPartitionCode()) {
            logger.error("UnknownTopicOrPartitionCode error - re-init-ing Kafka for partition {}", partition);
            reInitKafka();
        }
        return null;
    }

    public void close() {
    	if (simpleConsumer != null)
    		simpleConsumer.close();
        logger.info("Consumer instance is closed");
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

}
