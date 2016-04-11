/**
  * @author marinapopova
  * Feb 24, 2016
 */
package org.elasticsearch.kafka.indexer.examples;

import org.elasticsearch.kafka.indexer.service.ElasticSearchBatchService;
import org.elasticsearch.kafka.indexer.service.ElasticSearchClientService;
import org.elasticsearch.kafka.indexer.service.IMessageHandler;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;

/**
 * 
 * This is an example of a customized Message Handler - by implementing IMessageHandler interface
 * and using the BasicMessageHandler to delegate most of the non-customized methods
 *
 */
public class SimpleMessageHandlerImpl implements IMessageHandler {

	private ElasticSearchBatchService elasticSearchBatchService = null;
	@Autowired
	private ElasticSearchClientService elasticSearchClientService;
	private String indexName;
	private String indexType;

	@Override
	public byte[] transformMessage(byte[] inputMessage, Long offset) throws Exception {
		return inputMessage;
	}

	@PostConstruct
	public void init(){
		elasticSearchBatchService =new ElasticSearchBatchService(elasticSearchClientService);
	}

	@Override
	public void addMessageToBatch(byte[] inputMessage, Long offset) throws Exception {
		elasticSearchBatchService.addEventToBulkRequest(new String(inputMessage),indexName,indexType,null,null);

	}

	@Override
	public boolean postToElasticSearch() throws Exception {
		return elasticSearchBatchService.postToElasticSearch();
	}
}
