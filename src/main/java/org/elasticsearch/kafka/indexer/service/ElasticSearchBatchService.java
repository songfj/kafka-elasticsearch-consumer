package org.elasticsearch.kafka.indexer.service;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.kafka.indexer.exception.IndexerESException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by dhyan on 4/11/16.
 */
@Service
@Scope(value = BeanDefinition.SCOPE_PROTOTYPE)
public class ElasticSearchBatchService {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchBatchService.class);

    private Map<String, BulkRequestBuilder> bulkRequestBuilders;
    @Autowired
    private ElasticSearchClientService elasticSearchClientService;

    /**
     * 
     * @param inputMessage - message body 	
     * @param indexName - ES index name to index this event into 
     * @param indexType - index type of the ES 
     * @param eventUUID - uuid of the event - if needed for routing or as a UUID to use for ES documents; can be NULL
     * @param routingValue - value to use for ES index routing - if needed; can be null if routing is not needed 
     * @throws ExecutionException
     */
    public void addEventToBulkRequest(String inputMessage, String indexName, String indexType, String eventUUID, String routingValue) throws ExecutionException {
        BulkRequestBuilder builderForThisIndex = getBulkRequestBuilder(indexName);
        IndexRequestBuilder indexRequestBuilder = elasticSearchClientService.prepareIndex(indexName, indexType, eventUUID);
        indexRequestBuilder.setSource(inputMessage);
        if (StringUtils.isNotBlank(routingValue)) {
            indexRequestBuilder.setRouting(routingValue);
        }
        builderForThisIndex.add(indexRequestBuilder);
    }

    public boolean postToElasticSearch() throws Exception {
        try {
            for (Map.Entry<String, BulkRequestBuilder> entry : bulkRequestBuilders.entrySet()) {
                BulkRequestBuilder bulkRequestBuilder = entry.getValue();
                postBulkToEs(bulkRequestBuilder);
                logger.info("Bulk-posting to ES for index: {} # of messages: {}",
                        entry.getKey(), bulkRequestBuilder.numberOfActions());
            }
        } finally {
            bulkRequestBuilders.clear();
        }
        return true;
    }

    /**
     * Add initialization of the hashmap here as well, to enable unit testing of individual methods of this class
     * without calling prepareForPostToElasticSearch() as the first method always
     * @param key
     * @return
     */
    private BulkRequestBuilder getBulkRequestBuilder(String key) {
        if (bulkRequestBuilders == null)
            bulkRequestBuilders = new HashMap<>();
        BulkRequestBuilder bulkRequestBuilder = bulkRequestBuilders.get(key);
        if (bulkRequestBuilder == null) {
            bulkRequestBuilder = elasticSearchClientService.prepareBulk();
            bulkRequestBuilders.put(key, bulkRequestBuilder);
        }
        return bulkRequestBuilder;
    }

    private void postBulkToEs(BulkRequestBuilder bulkRequestBuilder)
            throws InterruptedException, IndexerESException {
        BulkResponse bulkResponse = null;
        BulkItemResponse bulkItemResp = null;
        //Nothing/NoMessages to post to ElasticSearch
        if (bulkRequestBuilder.numberOfActions() <= 0) {
            logger.warn("No messages to post to ElasticSearch - returning");
            return;
        }
        try {
            bulkResponse = bulkRequestBuilder.execute().actionGet();
        } catch (NoNodeAvailableException e) {
            // ES cluster is unreachable or down. Re-try up to the configured number of times
            // if fails even after then - throw an exception out to retry indexing the batch
            logger.error("Error posting messages to ElasticSearch: " +
                    "NoNodeAvailableException - ES cluster is unreachable, will try to re-connect after sleeping ... ", e);
            elasticSearchClientService.reInitElasticSearch();
            //even if re-init of ES succeeded - throw an Exception to re-process the current batch
            throw new IndexerESException("Recovering after an NoNodeAvailableException posting messages to Elastic Search " +
                    " - will re-try processing current batch");
        } catch (ElasticsearchException e) {
            logger.error("Failed to post messages to ElasticSearch: " + e.getMessage(), e);
            throw e;
        }
        logger.debug("Time to post messages to ElasticSearch: {} ms", bulkResponse.getTookInMillis());
        if (bulkResponse.hasFailures()) {
            logger.error("Bulk Message Post to ElasticSearch has errors: {}",
                    bulkResponse.buildFailureMessage());
            int failedCount = 0;
            Iterator<BulkItemResponse> bulkRespItr = bulkResponse.iterator();
            //TODO research if there is a way to get all failed messages without iterating over
            // ALL messages in this bulk post request
            while (bulkRespItr.hasNext()) {
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

    public Map<String, BulkRequestBuilder> getBulkRequestBuilders() {
        return bulkRequestBuilders;
    }

    public ElasticSearchClientService getElasticSearchClientService() {
        return elasticSearchClientService;
    }

    public void setElasticSearchClientService(ElasticSearchClientService elasticSearchClientService) {
        this.elasticSearchClientService = elasticSearchClientService;
    }
}
