/**
  * @author marinapopova
  * Feb 24, 2016
 */
package org.elasticsearch.kafka.indexer.service.impl.examples;

import org.elasticsearch.kafka.indexer.service.ElasticSearchBatchService;
import org.elasticsearch.kafka.indexer.service.IMessageHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 * 
 * This is an example of a customized Message Handler - by implementing IMessageHandler interface
 * and using the ElasticSearchBatchService to delegate most of the non-customized logic
 *
 */
public class SimpleMessageHandlerImpl implements IMessageHandler {

	@Autowired
	private ElasticSearchBatchService elasticSearchBatchService = null;
	@Value("${esIndexType:indexName}")
	private String indexName;
	@Value("${esIndexType:varnish}")
	private String indexType;

	@Override
	public byte[] transformMessage(byte[] inputMessage, Long offset) throws Exception {
		// do not do any transformations for this scenario - just return the message as is
		return inputMessage;
	}

	@Override
	public void addMessageToBatch(byte[] inputMessage, Long offset) throws Exception {
		String eventUUID = null; // we don't need a UUID for this simple scenario
		String routingValue = null; // we don't need routing for this simple scenario		
		elasticSearchBatchService.addEventToBulkRequest(
			new String(inputMessage), indexName, indexType, eventUUID, routingValue);
	}

	@Override
	public boolean postToElasticSearch() throws Exception {
		return elasticSearchBatchService.postToElasticSearch();
	}
}
