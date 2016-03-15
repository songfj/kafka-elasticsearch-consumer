package org.elasticsearch.kafka.indexer.service.impl;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;

import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.kafka.indexer.FailedEventsLogger;
import org.elasticsearch.kafka.indexer.service.ElasticSearchClientService;
import org.elasticsearch.kafka.indexer.service.IIndexHandler;
import org.elasticsearch.kafka.indexer.service.IMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

public class BasicMessageHandler implements IMessageHandler {

    @Autowired
    private ElasticSearchClientService elasticSearchClientService;
    @Autowired
	@Qualifier("indexHandler")
    private IIndexHandler elasticIndexHandler;
    
    private TransportClient elasticSearchClient;
	private static final Logger logger = LoggerFactory.getLogger(BasicMessageHandler.class);
	private Map<String, BulkRequestBuilder> bulkRequestBuilders;
	
    @PostConstruct
    public void init() {
        elasticSearchClient = elasticSearchClientService.getElasticSearchClient() ;
    }

	public long prepareForPostToElasticSearch(Iterator<MessageAndOffset> messageAndOffsetIterator){
		bulkRequestBuilders = new HashMap<>();
		int numProcessedMessages = 0;
		int numSkippedIndexingMessages = 0;
		int numMessagesInBatch = 0;
		long offsetOfNextBatch = 0;
		while(messageAndOffsetIterator.hasNext()) {
			numMessagesInBatch++;
			MessageAndOffset messageAndOffset = messageAndOffsetIterator.next();
			offsetOfNextBatch = messageAndOffset.nextOffset();
			Message message = messageAndOffset.message();
			ByteBuffer payload = message.payload();
			byte[] bytesMessage = new byte[payload.limit()];
			payload.get(bytesMessage);

			try {
				processMessage(bytesMessage);
				numProcessedMessages++;
			} catch (Exception e) {
				numSkippedIndexingMessages++;
				String msgStr = new String(bytesMessage);
				logger.error("ERROR processing message at offset={} - skipping it: {}",messageAndOffset.offset(), msgStr, e);
				FailedEventsLogger.logFailedToTransformEvent(messageAndOffset.offset(), e.getMessage(), msgStr);
			}
		}
		logger.info("Total # of messages in this batch: {}; " +
                        "# of successfully transformed and added to Index: {}; # of skipped from indexing: {}; offsetOfNextBatch: {}",
                numMessagesInBatch, numProcessedMessages, numSkippedIndexingMessages, offsetOfNextBatch);
		return offsetOfNextBatch;
	}


	public void processMessage(byte[] bytesMessage) throws Exception {
		// customize this behavior as needed in your own MessageHandler implementation class
		byte[] transformedBytesMessage = transformMessage(bytesMessage, null);
		String indexName = elasticIndexHandler.getIndexName(null);
		String indexType = elasticIndexHandler.getIndexType(null);
		boolean needsRouting = false;
		String messageUUID = null;
		String messageStr = new String(transformedBytesMessage);
		logger.debug("Adding event to the ES Index builder for indexName={}; event=[[{}]]", 
				indexName, messageStr);
		addEventToBulkRequest(indexName, indexType, messageUUID, needsRouting, null, messageStr);
	}


	public void addEventToBulkRequest(String indexName, String indexType, 
			String eventUUID, boolean needsRouting, String routingValue, String jsonEvent) throws ExecutionException {
 		BulkRequestBuilder builderForThisIndex = getBulkRequestBuilder(indexName);
 		IndexRequestBuilder indexRequestBuilder = null;
 		// if uuid for messages is provided - index with uuid
 		if (StringUtils.isNotEmpty(eventUUID)) {
		indexRequestBuilder = elasticSearchClient.prepareIndex(
				indexName, indexType, eventUUID);
 		} else {
 			indexRequestBuilder = elasticSearchClient.prepareIndex(indexName, indexType);			
 		}
		indexRequestBuilder.setSource(jsonEvent);
		if(needsRouting){
			indexRequestBuilder.setRouting(routingValue);
		}
		builderForThisIndex.add(indexRequestBuilder);
    }

    public BulkRequestBuilder getBulkRequestBuilder(String key){
		BulkRequestBuilder bulkRequestBuilder = bulkRequestBuilders.get(key);
		if (bulkRequestBuilder == null) {
			bulkRequestBuilder = elasticSearchClient.prepareBulk();
			bulkRequestBuilders.put(key, bulkRequestBuilder);
		}
		return bulkRequestBuilder;
	}
	

	@Override
	public boolean postToElasticSearch() throws Exception {
		for (Map.Entry<String, BulkRequestBuilder> entry: bulkRequestBuilders.entrySet()){
			BulkRequestBuilder bulkRequestBuilder = entry.getValue();
			postOneBulkRequestToES(bulkRequestBuilder);
			logger.info("Bulk-posting to ES for index: {} # of messages: {}",
					entry.getKey(), bulkRequestBuilder.numberOfActions());
		}
		return true;
	}

    private void postOneBulkRequestToES(BulkRequestBuilder bulkRequestBuilder) {
        BulkResponse bulkResponse = null;
        BulkItemResponse bulkItemResp = null;
        //Nothing/NoMessages to post to ElasticSearch
        if(bulkRequestBuilder.numberOfActions() <= 0){
            logger.warn("No messages to post to ElasticSearch - returning");
            return;
        }
        try{
            bulkResponse = bulkRequestBuilder.execute().actionGet();
        }
        catch(ElasticsearchException e){
            logger.error("Failed to post messages to ElasticSearch: " + e.getMessage(), e);
            throw e;
        }
        logger.debug("Time to post messages to ElasticSearch: {} ms", bulkResponse.getTookInMillis());
        if(bulkResponse.hasFailures()){
            logger.error("Bulk Message Post to ElasticSearch has errors: {}",
                    bulkResponse.buildFailureMessage());
            int failedCount = 0;
            Iterator<BulkItemResponse> bulkRespItr = bulkResponse.iterator();
            //TODO research if there is a way to get all failed messages without iterating over
            // ALL messages in this bulk post request
            while (bulkRespItr.hasNext()){
                bulkItemResp = bulkRespItr.next();
                if (bulkItemResp.isFailed()) {
                    failedCount++;
                    String errorMessage = bulkItemResp.getFailure().getMessage();
                    String restResponse = bulkItemResp.getFailure().getStatus().name();
                    logger.error("Failed Message #{}, REST response:{}; errorMessage:{}",
                            failedCount, restResponse, errorMessage);
                    // TODO: there does not seem to be a way to get the actual failed event
                    // until it is possible - do not log anything into the failed events log file
                    //FailedEventsLogger.logFailedToPostToESEvent(restResponse, errorMessage);
                }
            }
            logger.info("# of failed to post messages to ElasticSearch: {} ", failedCount);
        } else {
        	logger.info("Bulk Post to ElasticSearch finished OK");
        }
    }

	/* (non-Javadoc)
	 * @see org.elasticsearch.kafka.indexer.service.IMessageHandler#transformMessage(byte[], java.lang.Long)
	 */
	@Override
	public byte[] transformMessage(byte[] inputMessage, Long offset)
			throws Exception {
		// TODO customize this if necessary
		return inputMessage;
	}

}
