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
 * and using the BasicMessageHandler to delegate most of the non-customized methods
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
		return inputMessage;
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
