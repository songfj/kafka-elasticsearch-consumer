package org.elasticsearch.kafka.indexer.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

/**
 * Created by dhyan on 1/29/16.
 */
@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RawLogMessageHandlerServiceImpl extends GenericBulkESMessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(RawLogMessageHandlerServiceImpl.class);


    @Override
    public byte[] transformMessage(byte[] inputMessage, Long offset) throws Exception {
        // do necessary transformation here
        // in the simplest case - post as is
        byte[]  outputMessage = inputMessage;
        return outputMessage;
    }


}
