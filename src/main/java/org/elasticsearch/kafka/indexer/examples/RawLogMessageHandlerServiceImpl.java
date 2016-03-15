package org.elasticsearch.kafka.indexer.examples;

import org.elasticsearch.kafka.indexer.service.impl.BasicMessageHandler;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

/**
 * Created by dhyan on 1/29/16.
 * 
 * This is an example of a customized Message Handler - via extending the BasicMessageHandler
 * 
 */
@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RawLogMessageHandlerServiceImpl extends BasicMessageHandler {


    @Override
    public byte[] transformMessage(byte[] inputMessage, Long offset) throws Exception {
        // do necessary transformation here
        // in the simplest case - post as is
        //byte[]  outputMessage = inputMessage;
        return inputMessage;
    }


}
