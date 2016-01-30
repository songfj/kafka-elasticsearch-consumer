package org.elasticsearch.kafka.indexer.service.inter;

import kafka.message.MessageAndOffset;

import java.util.Iterator;

/**
 * Created by dhyan on 1/28/16.
 */
public interface MessageHandlerService {
    boolean postToElasticSearch() throws Exception ;
    long prepareForPostToElasticSearch(Iterator<MessageAndOffset> messageAndOffsetIterator) ;
    byte[] transformMessage( byte[] inputMessage, Long offset) throws Exception ;

}
