/**
  * @author marinapopova
  * Feb 24, 2016
 */
package org.elasticsearch.kafka.indexer.examples;

import java.util.Iterator;

import kafka.message.MessageAndOffset;

import org.elasticsearch.kafka.indexer.service.IMessageHandler;
import org.elasticsearch.kafka.indexer.service.impl.BasicMessageHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * 
 * This is an example of a customized Message Handler - by implementing IMessageHandler interface
 * and using the BasicMessageHandler to delegate most of the non-customized methods
 *
 */
public class SimpleMessageHandlerImpl implements IMessageHandler {

	@Autowired 
	@Qualifier("messageHandler")
	BasicMessageHandler basicHandler;
	
	/**
	 * 
	 */
	public SimpleMessageHandlerImpl() {
	}

	/* (non-Javadoc)
	 * @see org.elasticsearch.kafka.indexer.service.IMessageHandler#postToElasticSearch()
	 */
	@Override
	public boolean postToElasticSearch() throws Exception {
		return basicHandler.postToElasticSearch();
	}

	/* (non-Javadoc)
	 * @see org.elasticsearch.kafka.indexer.service.IMessageHandler#prepareForPostToElasticSearch(java.util.Iterator)
	 */
	@Override
	public long prepareForPostToElasticSearch(
			Iterator<MessageAndOffset> messageAndOffsetIterator) {
		return basicHandler.prepareForPostToElasticSearch(messageAndOffsetIterator);
	}

	/* (non-Javadoc)
	 * @see org.elasticsearch.kafka.indexer.service.IMessageHandler#transformMessage(byte[], java.lang.Long)
	 */
	@Override
	public byte[] transformMessage(byte[] inputMessage, Long offset)
			throws Exception {
		// TODO do your custom transformation here if needed
		// or return the message as is
		return inputMessage;
	}

	/* (non-Javadoc)
	 * @see org.elasticsearch.kafka.indexer.service.IMessageHandler#processMessage(byte[])
	 */
	@Override
	public void processMessage(byte[] bytesMessage) throws Exception {
		basicHandler.processMessage(bytesMessage);
	}

}
