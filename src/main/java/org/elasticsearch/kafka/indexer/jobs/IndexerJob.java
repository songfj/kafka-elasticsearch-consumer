package org.elasticsearch.kafka.indexer.jobs;

import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.kafka.indexer.FailedEventsLogger;
import org.elasticsearch.kafka.indexer.exception.IndexerESException;
import org.elasticsearch.kafka.indexer.exception.KafkaClientNotRecoverableException;
import org.elasticsearch.kafka.indexer.exception.KafkaClientRecoverableException;
import org.elasticsearch.kafka.indexer.service.IMessageHandler;
import org.elasticsearch.kafka.indexer.service.KafkaClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.Callable;

public class IndexerJob implements Callable<IndexerJobStatus> {

    private static final Logger logger = LoggerFactory.getLogger(IndexerJob.class);
    private IMessageHandler messageHandlerService;
    public KafkaClientService kafkaClient;
    private long offsetForThisRound;
    private long nextOffsetToProcess;
    private boolean isStartingFirstTime;
    private final int currentPartition;
    private final String currentTopic;
    private IndexerJobStatus indexerJobStatus;
    private volatile boolean shutdownRequested = false;
    private int consumerSleepBetweenFetchsMs;
    // this property can be set to TRUE to enable logging timings of the event processing
    private boolean isPerfReportingEnabled = false;
    // this property can be set to TRUE to skip indexing into ES
    private boolean isDryRun = false;
   
    public IndexerJob(String topic, IMessageHandler messageHandlerService,
                      KafkaClientService kafkaClient, int partition, int consumerSleepBetweenFetchsMs)
            throws Exception {
        this.currentPartition = partition;
        this.currentTopic = topic;
        this.messageHandlerService = messageHandlerService;
        indexerJobStatus = new IndexerJobStatus(-1L, IndexerJobStatusEnum.Created, partition);
        isStartingFirstTime = true;
        this.consumerSleepBetweenFetchsMs = consumerSleepBetweenFetchsMs;
        this.kafkaClient = kafkaClient;
        indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Initialized);
        logger.info("Created IndexerJob for topic={}, partition={};  messageHandlerService={}; kafkaClient={}",
                currentTopic, partition, messageHandlerService, kafkaClient);
    }

    // a hook to be used by the Manager app to request a graceful shutdown of the job
    public void requestShutdown() {
        shutdownRequested = true;
    }

    public IndexerJobStatus call() {
        indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Started);
        while (!shutdownRequested) {
            try {
                if (Thread.currentThread().isInterrupted()) {
                    Thread.currentThread().interrupt();
                    throw new InterruptedException(
                            "Caught interrupted event in IndexerJob for partition=" + currentPartition + " - stopping");
                }
                logger.debug("******* Starting a new batch of events from Kafka for partition {} ...", currentPartition);
                processMessagesFromKafka();
                indexerJobStatus.setJobStatus(IndexerJobStatusEnum.InProgress);
                Thread.sleep(consumerSleepBetweenFetchsMs * 1000);
                logger.debug("Completed a round of indexing into ES for partition {}", currentPartition);
            } catch (IndexerESException | KafkaClientNotRecoverableException e) {
                indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Failed);
                stopClients();
                break;
            } catch (InterruptedException e) {
                indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Stopped);
                stopClients();
                break;
            } catch (Exception e) {
                if (!reinitKafkaSucessful(e)) {
                    break;
                }

            }
        }
        logger.warn("******* Indexing job was stopped, indexerJobStatus={} - exiting", indexerJobStatus);
        return indexerJobStatus;
    }

    /**
     * Try to re-init Kafka connection first - in case the leader for this partition
     * has changed due to a Kafka node restart and/or leader re-election
     * TODO decide if we want to re-try forever or fail here
     * TODO introduce another JobStatus to indicate that the job is in the REINIT state - if this state can take awhile
     * If Exception is thrown dureing reinit stop the job and fix the issue manually,
     * It is better to monitor the job externally
     * via Zabbix or the likes - rather then keep failing [potentially] forever
     * @param e
     * @return
     */
    private boolean reinitKafkaSucessful(Exception e) {
        // we will treat all other Exceptions as recoverable for now
        logger.error("Exception when starting a new round of kafka Indexer job for partition {} - will try to re-init Kafka ",
                currentPartition, e);
        try {
            kafkaClient.reInitKafka();
            return true;
        } catch (Exception e2) {
            logger.error("Exception when starting a new round of kafka Indexer job, partition {}, exiting: "
                    + e2.getMessage(), currentPartition);
            indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Failed);
            stopClients();
            return false;
        }
    }

    /**
     * save nextOffsetToProcess in temporary field,and save it after successful execution of indexIntoESWithRetries method
     * @throws Exception
     */
    public void processMessagesFromKafka() throws Exception {
        long jobStartTime = System.currentTimeMillis();
        determineOffsetForThisRound(jobStartTime);
        ByteBufferMessageSet byteBufferMsgSet = getMessageAndOffsets(jobStartTime);

        if (byteBufferMsgSet != null) {
            logger.debug("Starting to prepare for post to ElasticSearch for partition {}", currentPartition);
            long proposedNextOffsetToProcess = addMessagesToBatch(jobStartTime, byteBufferMsgSet);
            if (isDryRun) {
                logger.info("**** This is a dry run, NOT committing the offset in Kafka nor posting to ES for partition {}****", currentPartition);
                return;
            }
            boolean isPostSuccessful = postBatchToElasticSearch(proposedNextOffsetToProcess);
            if (isPostSuccessful) {
                commitOffSet(jobStartTime);
            }
        }
    }

    /**
     * 1) Do not handle exceptions here - throw them out to the call() method which will decide if re-init
     * @param jobStartTime
     * @throws KafkaClientRecoverableException
     */
    private void commitOffSet(long jobStartTime) throws KafkaClientRecoverableException {
        if (isPerfReportingEnabled) {
            long timeAfterEsPost = System.currentTimeMillis();
            logger.debug("Approx time to post of ElasticSearch: {} ms for partition {}",
                    (timeAfterEsPost - jobStartTime), currentPartition);
        }
        logger.info("Committing offset={} for partition={}", nextOffsetToProcess, currentPartition);
        try {
            kafkaClient.saveOffsetInKafka(nextOffsetToProcess, ErrorMapping.NoError());
        } catch (Exception e) {
            logger.error("Failed to commit nextOffsetToProcess={} after processing and posting to ES for partition={}: ",
                    nextOffsetToProcess, currentPartition, e);
            throw new KafkaClientRecoverableException("Failed to commit nextOffsetToProcess=" + nextOffsetToProcess +
                    " after processing and posting to ES; partition=" + currentPartition + "; error: " + e.getMessage(), e);
        }

        if (isPerfReportingEnabled) {
            long timeAtEndOfJob = System.currentTimeMillis();
            logger.info("*** This round of IndexerJob took about {} ms for partition {} ",
                    (timeAtEndOfJob - jobStartTime), currentPartition);
        }
        logger.info("*** Finished current round of IndexerJob, processed messages with offsets [{}-{}] for partition {} ****",
                offsetForThisRound, nextOffsetToProcess, currentPartition);
    }

    /**
     * TODO: we are loosing the ability to set Job's status to HANGING in case ES is unreachable and
     * re-connect to ES takes awhile ... See if it is possible to re-introduce it in another way
     * ElasticsearchException e :
     *      we are assuming that these exceptions are data-specific - continue and commit the offset,
     *      but be aware that ALL messages from this batch are NOT indexed into ES
     * @param proposedNextOffsetToProcess
     * @return
     * @throws Exception
     */
    private boolean postBatchToElasticSearch(long proposedNextOffsetToProcess) throws Exception {
        try {
            logger.info("About to post messages to ElasticSearch for partition={}, offsets {}-->{} ",
                    currentPartition, offsetForThisRound, proposedNextOffsetToProcess - 1);
            messageHandlerService.postToElasticSearch();
        } catch (IndexerESException e) {
            logger.error("Error posting messages to Elastic Search for offsets {}-->{} " +
                            " in partition={} - will re-try processing the batch; error: {}",
                    offsetForThisRound, proposedNextOffsetToProcess - 1, currentPartition, e.getMessage());
            return false;
        } catch (ElasticsearchException e) {
            logger.error("Error posting messages to ElasticSearch for offset {}-->{} in partition {} skipping them: ",
                    offsetForThisRound, proposedNextOffsetToProcess - 1, currentPartition, e);
            FailedEventsLogger.logFailedEvent(offsetForThisRound, proposedNextOffsetToProcess - 1, currentPartition, e.getDetailedMessage(), null);
        }

        nextOffsetToProcess = proposedNextOffsetToProcess;
        return true;
    }

    private long addMessagesToBatch(long jobStartTime, ByteBufferMessageSet byteBufferMsgSet) {
        int numProcessedMessages = 0;
        int numSkippedIndexingMessages = 0;
        int numMessagesInBatch = 0;
        long offsetOfNextBatch = 0;
        Iterator<MessageAndOffset> messageAndOffsetIterator = byteBufferMsgSet.iterator();

        while (messageAndOffsetIterator.hasNext()) {
            numMessagesInBatch++;
            MessageAndOffset messageAndOffset = messageAndOffsetIterator.next();
            offsetOfNextBatch = messageAndOffset.nextOffset();
            Message message = messageAndOffset.message();
            ByteBuffer payload = message.payload();
            byte[] bytesMessage = new byte[payload.limit()];
            payload.get(bytesMessage);
            try {
                byte[] transformedMessage = messageHandlerService.transformMessage(bytesMessage,messageAndOffset.offset());
                messageHandlerService.addMessageToBatch(transformedMessage,messageAndOffset.offset());
                numProcessedMessages++;
            } catch (Exception e) {
                numSkippedIndexingMessages++;
                String msgStr = new String(bytesMessage);
                logger.error("ERROR processing message at offset={} - skipping it: {}", messageAndOffset.offset(), msgStr, e);
                FailedEventsLogger.logFailedToTransformEvent(messageAndOffset.offset(), e.getMessage(), msgStr);
            }
        }
        logger.info("Total # of messages in this batch: {}; " +
                        "# of successfully transformed and added to Index: {}; # of skipped from indexing: {}; offsetOfNextBatch: {}",
                numMessagesInBatch, numProcessedMessages, numSkippedIndexingMessages, offsetOfNextBatch);

        if (isPerfReportingEnabled) {
            long timeAtPrepareES = System.currentTimeMillis();
            logger.debug("Completed preparing for post to ElasticSearch. Approx time taken: {}ms for partition {}",
                    (timeAtPrepareES - jobStartTime), currentPartition);
        }
        return offsetOfNextBatch;
    }

    /**
     * 1) Exceptions will be taken care of in the computeOffset() and thrown to call()method
     *      which will decide if re-init
     * 2) Roll back to the earliest offset in the case of OffsetOutOfRange
     * CORNER CASE :
     * TODO re-review this logic
     *  check a corner case when consumer did not read any events form Kafka from the last current offset -
     *  but the latestOffset reported by Kafka is higher than what consumer is trying to read from;
     * @param jobStartTime
     * @return
     * @throws Exception
     */
    private ByteBufferMessageSet getMessageAndOffsets(long jobStartTime) throws Exception {
        FetchResponse fetchResponse = kafkaClient.getMessagesFromKafka(offsetForThisRound);
        ByteBufferMessageSet byteBufferMsgSet = null;
        if (fetchResponse.hasError()) {
            short errorCode = fetchResponse.errorCode(currentTopic, currentPartition);
            Long newNextOffsetToProcess = kafkaClient.handleErrorFromFetchMessages(errorCode, offsetForThisRound);
            if (newNextOffsetToProcess != null) {
                nextOffsetToProcess = newNextOffsetToProcess;
            }
            return null;
        }

        byteBufferMsgSet = fetchResponse.messageSet(currentTopic, currentPartition);
        if (isPerfReportingEnabled) {
            long timeAfterKafkaFetch = System.currentTimeMillis();
            logger.debug("Completed MsgSet fetch from Kafka. Approx time taken is {} ms for partition {}",
                    (timeAfterKafkaFetch - jobStartTime), currentPartition);
        }
        if (byteBufferMsgSet.validBytes() <= 0) {
            logger.debug("No events were read from Kafka - finishing this round of reads from Kafka for partition {}", currentPartition);
            long latestOffset = kafkaClient.getLastestOffset();
            if (latestOffset != offsetForThisRound) {
                logger.warn("latestOffset={} for partition={} is not the same as the offsetForThisRound for this run: {}" +
                                " - returning; will try reading messages form this offset again ",
                        latestOffset, currentPartition, offsetForThisRound);
                // TODO decide if we really need to do anything here - for now:
            }
            byteBufferMsgSet = null;
        }
        return byteBufferMsgSet;
    }

    /**
     * 1) Do not read offset from Kafka after each run - instead from memory
     * 2) Do not handle exceptions here - they will be taken care of in the computeOffset()
     *     If this is the only thread that is processing data from this partition
     * TODO  see if we are doing this too early - before we actually commit the offset
     * @param jobStartTime
     * @throws Exception
     */
    private void determineOffsetForThisRound(long jobStartTime) throws Exception {

        if (!isStartingFirstTime) {
            offsetForThisRound = nextOffsetToProcess;
        } else {
            indexerJobStatus.setJobStatus(IndexerJobStatusEnum.InProgress);
            offsetForThisRound = kafkaClient.computeInitialOffset();
            isStartingFirstTime = false;
            nextOffsetToProcess = offsetForThisRound;
        }
        indexerJobStatus.setLastCommittedOffset(offsetForThisRound);
        return;
    }

    public void stopClients() {
        logger.info("About to stop Kafka client for topic {}, partition {}", currentTopic, currentPartition);
        if (kafkaClient != null)
            kafkaClient.close();
        logger.info("Stopped Kafka client for topic {}, partition {}", currentTopic, currentPartition);
    }

    public IndexerJobStatus getIndexerJobStatus() {
        return indexerJobStatus;
    }

	public void setPerfReportingEnabled(boolean isPerfReportingEnabled) {
		this.isPerfReportingEnabled = isPerfReportingEnabled;
	}

	public void setDryRun(boolean isDryRun) {
		this.isDryRun = isDryRun;
	}

}
