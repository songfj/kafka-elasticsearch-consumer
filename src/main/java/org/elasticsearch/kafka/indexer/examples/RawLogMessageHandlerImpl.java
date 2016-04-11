package org.elasticsearch.kafka.indexer.examples;

import org.elasticsearch.kafka.indexer.service.ElasticSearchBatchService;
import org.elasticsearch.kafka.indexer.service.ElasticSearchClientService;
import org.elasticsearch.kafka.indexer.service.IMessageHandler;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;

/**
 * Created by dhyan on 1/29/16.
 * 
 * This is an example of a customized Message Handler - via extending the BasicMessageHandler
 * 
 */

public class RawLogMessageHandlerImpl implements IMessageHandler {
    private ElasticSearchBatchService elasticSearchBatchService = null;
    @Autowired
    private ElasticSearchClientService elasticSearchClientService;

    @PostConstruct
    public void init(){
        elasticSearchBatchService =new ElasticSearchBatchService(elasticSearchClientService);
    }


    @Override
    public void addMessageToBatch(byte[] inputMessage, Long offset) throws Exception {
        elasticSearchBatchService.addEventToBulkRequest(new String(inputMessage),"raw-index","raw-type",null,null);
    }

    @Override
    public byte[] transformMessage(byte[] inputMessage, Long offset) throws Exception {
        return inputMessage;
    }

    @Override
    public boolean postToElasticSearch() throws Exception {
        return elasticSearchBatchService.postToElasticSearch();
    }
}