package org.elasticsearch.kafka.indexer.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by dhyan on 1/29/16.
 */

public class AccessLogMessageHandlerServiceImpl extends GenericBulkESMessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(AccessLogMessageHandlerServiceImpl.class);


    @Override
    public byte[] transformMessage(byte[] inputMessage, Long offset) throws Exception {
        String outputMessageStr = messageConversionService.convertToJson(new String(inputMessage, "UTF-8"), offset);
        return outputMessageStr.getBytes();
    }


}
