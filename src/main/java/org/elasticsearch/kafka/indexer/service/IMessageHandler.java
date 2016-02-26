package org.elasticsearch.kafka.indexer.service;

import kafka.message.MessageAndOffset;

import java.util.Iterator;

/**
 * Created by dhyan on 1/28/16.
 */
public interface IMessageHandler {
    public boolean postToElasticSearch() throws Exception ;
    public long prepareForPostToElasticSearch(Iterator<MessageAndOffset> messageAndOffsetIterator) ;
    public byte[] transformMessage( byte[] inputMessage, Long offset) throws Exception ;
    public void processMessage(byte[] bytesMessage) throws Exception;
}
