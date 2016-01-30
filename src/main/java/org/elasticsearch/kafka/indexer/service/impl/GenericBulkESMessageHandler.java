package org.elasticsearch.kafka.indexer.service.impl;

import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.kafka.indexer.logger.FailedEventsLogger;
import org.elasticsearch.kafka.indexer.service.ElasticSearchClientService;
import org.elasticsearch.kafka.indexer.service.helper.MessageConversionService;
import org.elasticsearch.kafka.indexer.service.inter.IndexHandlerService;
import org.elasticsearch.kafka.indexer.service.inter.MessageHandlerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Created by dhyan on 1/30/16.
 */
public abstract class GenericBulkESMessageHandler implements MessageHandlerService {
    private static final Logger logger = LoggerFactory.getLogger(GenericBulkESMessageHandler.class);

    @Autowired
    private ElasticSearchClientService elasticSearchClientService;

    @Autowired
    @Qualifier("messageConversionService")
    protected MessageConversionService messageConversionService;

    private TransportClient elasticSearchClient ;

    @Autowired
    private IndexHandlerService elasticIndexHandler ;

    private BulkRequestBuilder bulkRequestBuilder ;


    @PostConstruct
    public void init() {
        elasticSearchClient = elasticSearchClientService.getElasticSearchClient() ;
    }

    @Override
    public boolean postToElasticSearch() throws Exception {
        return bulkPostToElasticSearch();
    }
    @Override
    public abstract byte[] transformMessage(byte[] inputMessage, Long offset) throws Exception ;

    @Override
    public long prepareForPostToElasticSearch(Iterator<MessageAndOffset> messageAndOffsetIterator) {
        return prepareForBulkPost(messageAndOffsetIterator);
    }

    private boolean bulkPostToElasticSearch() {
        BulkResponse bulkResponse = null;
        BulkItemResponse bulkItemResp = null;
        //Nothing/NoMessages to post to ElasticSearch
        if(bulkRequestBuilder.numberOfActions() <= 0){
            logger.warn("No messages to post to ElasticSearch - returning");
            return true;
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
            return false;
        }
        logger.info("Bulk Post to ElasticSearch finished OK");
        bulkRequestBuilder = null;
        return true;
    }



    private long prepareForBulkPost(Iterator<MessageAndOffset> messageAndOffsetIterator) {
        bulkRequestBuilder = elasticSearchClient.prepareBulk();
        int numProcessedMessages = 0;
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
            byte[] transformedMessage;
            try {
                transformedMessage = transformMessage(bytesMessage, messageAndOffset.offset());
            } catch (Exception e) {
                String msgStr = new String(bytesMessage);
                logger.error("ERROR transforming message at offset={} - skipping it: {}",
                        messageAndOffset.offset(), msgStr, e);
                FailedEventsLogger.logFailedToTransformEvent(
                        messageAndOffset.offset(), e.getMessage(), msgStr);
                continue;
            }
            bulkRequestBuilder.add(elasticSearchClient.prepareIndex(
                            elasticIndexHandler.getIndexName(null), elasticIndexHandler.getIndexType(null))
                            .setSource(transformedMessage)
            );
            numProcessedMessages++;
        }
        logger.info("Total # of messages in this batch: {}; " +
                        "# of successfully transformed and added to Index messages: {}; offsetOfNextBatch: {}",
                numMessagesInBatch, numProcessedMessages, offsetOfNextBatch);
        return offsetOfNextBatch;
    }


}
