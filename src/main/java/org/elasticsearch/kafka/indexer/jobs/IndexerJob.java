package org.elasticsearch.kafka.indexer.jobs;

import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.message.ByteBufferMessageSet;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.kafka.indexer.FailedEventsLogger;
import org.elasticsearch.kafka.indexer.exception.IndexerESException;
import org.elasticsearch.kafka.indexer.exception.KafkaClientNotRecoverableException;
import org.elasticsearch.kafka.indexer.exception.KafkaClientRecoverableException;
import org.elasticsearch.kafka.indexer.service.ConsumerConfigService;
import org.elasticsearch.kafka.indexer.service.IMessageHandler;
import org.elasticsearch.kafka.indexer.service.KafkaClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class IndexerJob implements Callable<IndexerJobStatus> {

	private static final Logger logger = LoggerFactory.getLogger(IndexerJob.class);
	private ConsumerConfigService configService;
	private IMessageHandler messageHandlerService ;
	private TransportClient esClient;
	public KafkaClientService kafkaClient;
	private long offsetForThisRound;
	private long nextOffsetToProcess;
	private boolean isStartingFirstTime;
	private final int currentPartition;
	private final String currentTopic;
    private IndexerJobStatus indexerJobStatus;
    private volatile boolean shutdownRequested = false;


	public IndexerJob(ConsumerConfigService configService, IMessageHandler messageHandlerService, 
			KafkaClientService kafkaClient, int partition) 
			throws Exception {
		this.configService = configService;
		this.currentPartition = partition;
		this.currentTopic = configService.getTopic();
		this.messageHandlerService = messageHandlerService ;
		indexerJobStatus = new IndexerJobStatus(-1L, IndexerJobStatusEnum.Created, partition);
		isStartingFirstTime = true;
		this.kafkaClient = kafkaClient;
		initElasticSearch();
		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Initialized);
	}

	private void initElasticSearch() throws Exception {
		String[] esHostPortList = configService.getEsHostPortList().trim().split(",");
		logger.info("Initializing ElasticSearch: hostPortList={}, esClusterName={}, partition={}",
				esHostPortList, configService.getEsClusterName(), currentPartition);

		// TODO add validation of host:port syntax - to avoid Runtime exceptions
		try {
			Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", configService.getEsClusterName())
				.build();
			esClient = new TransportClient(settings);
			for (String eachHostPort : esHostPortList) {
				logger.info("adding [{}] to TransportClient for partition {} ... ", eachHostPort, currentPartition);
				esClient.addTransportAddress(
					new InetSocketTransportAddress(
						eachHostPort.split(":")[0].trim(), 
						Integer.parseInt(eachHostPort.split(":")[1].trim())
					)
				);
			}
			logger.info("ElasticSearch Client created and intialized OK for partition {}", currentPartition);
		} catch (Exception e) {
			logger.error("Exception trying to connect and create ElasticSearch Client: "
					+ e.getMessage());
			throw e;
		}
	}

	// a hook to be used by the Manager app to request a graceful shutdown of the job
	public void requestShutdown() {
		shutdownRequested = true;
	}

	public IndexerJobStatus call() {
		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Started);
        while(!shutdownRequested){
        	try{
        		// check if there was a request to stop this thread - stop processing if so
                if (Thread.currentThread().isInterrupted()){
                    // preserve interruption state of the thread
                    Thread.currentThread().interrupt();
                    throw new InterruptedException(
                    	"Cought interrupted event in IndexerJob for partition=" + currentPartition + " - stopping");
                }
        		logger.debug("******* Starting a new batch of events from Kafka for partition {} ...", currentPartition);
        		
        		processBatch();
        		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.InProgress);	
        		// sleep for configured time
        		// TODO improve sleep pattern
        		Thread.sleep(configService.getConsumerSleepBetweenFetchsMs() * 1000);
        		logger.debug("Completed a round of indexing into ES for partition {}",currentPartition);
        	} catch (IndexerESException e) {
        		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Failed);
        		stopClients();
        		break;
        	} catch (InterruptedException e) {
        		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Stopped);
        		stopClients();
        		break;
        	} catch (KafkaClientNotRecoverableException e) {
        		// this is a non-recoverable error - stop the indexer job
        		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Failed);
        		stopClients();
        		break;
        	} catch (Exception e){
        		// we will treat all other Exceptions as recoverable for now
        		logger.error("Exception when starting a new round of kafka Indexer job for partition {} - will try to re-init Kafka " ,
        				currentPartition, e);
        		// try to re-init Kafka connection first - in case the leader for this partition
        		// has changed due to a Kafka node restart and/or leader re-election
        		// TODO decide if we want to re-try forever or fail here
        		// TODO introduce another JobStatus to indicate that the job is in the REINIT state - if this state can take awhile
        		try {
        			kafkaClient.reInitKafka();
        		} catch (Exception e2) {
        			// we still failed - do not keep going anymore - stop and fix the issue manually,
            		// then restart the consumer again; It is better to monitor the job externally 
            		// via Zabbix or the likes - rather then keep failing [potentially] forever
            		logger.error("Exception when starting a new round of kafka Indexer job, partition {}, exiting: "
            				+ e2.getMessage(), currentPartition);
            		indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Failed);
            		stopClients();  
            		break;
        		}
        	}       
        }
		logger.warn("******* Indexing job was stopped, indexerJobStatus={} - exiting", indexerJobStatus);
		return indexerJobStatus;
	}
	
		
	public void processBatch() throws Exception {
		long jobStartTime = 0l;
		if (configService.isPerfReportingEnabled())
			jobStartTime = System.currentTimeMillis();
		if (!isStartingFirstTime) {
			// do not read offset from Kafka after each run - we just stored it there
			// If this is the only thread that is processing data from this partition - 
			// we can rely on the in-memory nextOffsetToProcess variable
			offsetForThisRound = nextOffsetToProcess;
		} else {
			indexerJobStatus.setJobStatus(IndexerJobStatusEnum.InProgress);
			// if this is the first time we run the Consumer - get it from Kafka
			// do not handle exceptions here - they will be taken care of in the computeOffset()
			// and exception will be thrown to the call() method which will decide if re-init
			// of Kafka should be attempted or not
			offsetForThisRound = kafkaClient.computeInitialOffset();
			// mark this as not first time startup anymore - since we already saved correct offset
			// to Kafka, and to avoid going through the logic of figuring out the initial offset
			// every round if it so happens that there were no events from Kafka for a long time
			isStartingFirstTime = false;
			nextOffsetToProcess = offsetForThisRound;
		}
		//TODO  see if we are doing this too early - before we actually commit the offset
		indexerJobStatus.setLastCommittedOffset(offsetForThisRound);
		
		// do not handle exceptions here - they will be taken care of in the computeOffset()
		// and exception will be thrown to the call() method which will decide if re-init
		// of Kafka should be attempted or not
		FetchResponse fetchResponse = kafkaClient.getMessagesFromKafka(offsetForThisRound);
		if (fetchResponse.hasError()) {
			// check what kind of error this is - for most errors, we will try to re-init Kafka;
			// in the case of OffsetOutOfRange - we may have to roll back to the earliest offset
			short errorCode = fetchResponse.errorCode(currentTopic, currentPartition);
			Long newNextOffsetToProcess = kafkaClient.handleErrorFromFetchMessages(errorCode, offsetForThisRound);
			if (newNextOffsetToProcess != null) {
				// this is the case when we have to re-set the nextOffsetToProcess
				nextOffsetToProcess = newNextOffsetToProcess;
			}
			// return - will try to re-process the batch again 
			return;
		}
		
		ByteBufferMessageSet byteBufferMsgSet = fetchResponse.messageSet(currentTopic, currentPartition);
		if (configService.isPerfReportingEnabled()) {
			long timeAfterKafkaFetch = System.currentTimeMillis();
			logger.debug("Completed MsgSet fetch from Kafka. Approx time taken is {} ms for partition {}",
				(timeAfterKafkaFetch - jobStartTime) ,currentPartition);
		}
		if (byteBufferMsgSet.validBytes() <= 0) {
			logger.debug("No events were read from Kafka - finishing this round of reads from Kafka for partition {}",currentPartition);
			// TODO re-review this logic
			// check a corner case when consumer did not read any events form Kafka from the last current offset - 
			// but the latestOffset reported by Kafka is higher than what consumer is trying to read from;
			long latestOffset = kafkaClient.getLastestOffset();
			if (latestOffset != offsetForThisRound) {
				logger.warn("latestOffset={} for partition={} is not the same as the offsetForThisRound for this run: {}" + 
					" - returning; will try reading messages form this offset again ", 
					latestOffset, currentPartition, offsetForThisRound);
				// TODO decide if we really need to do anything here - for now:
				// do not do anything, just return, and let the consumer try to read again from the same offset;
				// do not handle exceptions here - throw them out to the call() method which will decide if re-init
				// of Kafka should be attempted or not
				/*  
				try {
					kafkaClient.saveOffsetInKafka(latestOffset, ErrorMapping.NoError());
				} catch (Exception e) {
					logger.error("Failed to commit nextOffsetToProcess={} after processing and posting to ES for partition={}: ",
							nextOffsetToProcess, currentPartition, e);
					throw new KafkaClientRecoverableException("Failed to commit nextOffsetToProcess=" + nextOffsetToProcess + 
						" after processing and posting to ES; partition=" + currentPartition + "; error: " + e.getMessage(), e);
				}
				/*  */
			}
			return;
		}
		logger.debug("Starting to prepare for post to ElasticSearch for partition {}",currentPartition);
		//Need to save nextOffsetToProcess in temporary field, 
		//and save it after successful execution of indexIntoESWithRetries method 
		long proposedNextOffsetToProcess = messageHandlerService.prepareForPostToElasticSearch(byteBufferMsgSet.iterator());

		if (configService.isPerfReportingEnabled()) {
			long timeAtPrepareES = System.currentTimeMillis();
			logger.debug("Completed preparing for post to ElasticSearch. Approx time taken: {}ms for partition {}",
					(timeAtPrepareES - jobStartTime),currentPartition );
		}
		if (configService.isDryRun()) {
			logger.info("**** This is a dry run, NOT committing the offset in Kafka nor posting to ES for partition {}****",currentPartition);
			return;
		}

		try {
			this.indexIntoESWithRetries();
		} catch (IndexerESException e) {
			// re-process batch
			return;
		}
		
		nextOffsetToProcess = proposedNextOffsetToProcess;
		
		if (configService.isPerfReportingEnabled()) {
			long timeAfterEsPost = System.currentTimeMillis();
			logger.debug("Approx time to post of ElasticSearch: {} ms for partition {}",
					(timeAfterEsPost - jobStartTime),currentPartition);
		}
		logger.info("Commiting offset={} for partition={}", nextOffsetToProcess, currentPartition);
		// do not handle exceptions here - throw them out to the call() method which will decide if re-init
		// of Kafka should be attempted or not
		try {
			kafkaClient.saveOffsetInKafka(nextOffsetToProcess, ErrorMapping.NoError());
		} catch (Exception e) {
			logger.error("Failed to commit nextOffsetToProcess={} after processing and posting to ES for partition={}: ",
				nextOffsetToProcess, currentPartition, e);
			throw new KafkaClientRecoverableException("Failed to commit nextOffsetToProcess=" + nextOffsetToProcess + 
				" after processing and posting to ES; partition=" + currentPartition + "; error: " + e.getMessage(), e);
		}

		if (configService.isPerfReportingEnabled()) {
			long timeAtEndOfJob = System.currentTimeMillis();
			logger.info("*** This round of IndexerJob took about {} ms for partition {} ",
					(timeAtEndOfJob - jobStartTime),currentPartition);
		}
		logger.info("*** Finished current round of IndexerJob, processed messages with offsets [{}-{}] for partition {} ****",
				offsetForThisRound, nextOffsetToProcess, currentPartition);
	}

	private void reInitElasticSearch() throws InterruptedException, IndexerESException {
		for (int i=1; i<=configService.getNumberOfEsIndexingRetryAttempts(); i++ ){
			Thread.sleep(configService.getEsIndexingRetrySleepTimeMs());
			logger.warn("Retrying connect to ES and re-process batch, partition {}, try# {}", 
					currentPartition, i);
			try {
				this.initElasticSearch();
				// we succeeded - get out of the loop
				break;
			} catch (Exception e2) {
				if (i<configService.getNumberOfEsIndexingRetryAttempts()){
					// do not fail yet - will re-try again
					indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Hanging);
					logger.warn("Retrying connect to ES and re-process batch, partition {}, try# {} - failed again", 
							currentPartition, i);						
				} else {
					//we've exhausted the number of retries - throw a IndexerESException to stop the IndexerJob thread
					logger.error("Retrying connect to ES and re-process batch after connection failure, partition {}, "
							+ "try# {} - failed after the last retry; Will keep retrying, ", 
							currentPartition, i);						
					
					indexerJobStatus.setJobStatus(IndexerJobStatusEnum.Hanging);
					throw new IndexerESException("Indexing into ES failed due to connectivity issue to ES, partition: " +
						currentPartition);
				}
			}
		}
	}

	
	private void indexIntoESWithRetries() throws IndexerESException, Exception {
		try {
			logger.info("posting the messages to ElasticSearch for partition {}...",currentPartition);
			messageHandlerService.postToElasticSearch();
		} catch (NoNodeAvailableException e) {
			// ES cluster is unreachable or down. Re-try up to the configured number of times
			// if fails even after then - shutdown the current IndexerJob
			logger.error("Error posting messages to Elastic Search for offset {}-->{} " + 
					" in partition {}:  NoNodeAvailableException - ES cluster is unreachable, will retry to connect after sleeping for {}ms", 
					offsetForThisRound, nextOffsetToProcess-1, configService.getEsIndexingRetrySleepTimeMs(), currentPartition, e);
			
			reInitElasticSearch();
			//throws Exception to re-process current batch
			throw new IndexerESException();
			
		} catch (ElasticsearchException e) {
			// we are assuming that other exceptions are data-specific
			// -  continue and commit the offset, 
			// but be aware that ALL messages from this batch are NOT indexed into ES
			logger.error("Error posting messages to Elastic Search for offset {}-->{} in partition {} skipping them: ",
					offsetForThisRound, nextOffsetToProcess-1, currentPartition, e);
			FailedEventsLogger.logFailedEvent(offsetForThisRound, nextOffsetToProcess - 1, currentPartition, e.getDetailedMessage(), null);
		}
	
	}
	
	
	public void stopClients() {
		logger.info("About to stop ES client for topic {}, partition {}", 
				currentTopic, currentPartition);
		if (esClient != null)
			esClient.close();
		logger.info("About to stop Kafka client for topic {}, partition {}", 
				currentTopic, currentPartition);
		if (kafkaClient != null)
			kafkaClient.close();
		logger.info("Stopped Kafka and ES clients for topic {}, partition {}", 
				currentTopic, currentPartition);
	}
	
	public IndexerJobStatus getIndexerJobStatus() {
		return indexerJobStatus;
	}

}
