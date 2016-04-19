package org.elasticsearch.kafka.indexer.service.impl.examples;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.kafka.indexer.service.ElasticSearchBatchService;
import org.elasticsearch.kafka.indexer.service.IMessageHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by dhyan on 1/29/16.
 */

public class AccessLogMessageHandlerImpl implements IMessageHandler {

	// Kafka Topic to process messages from
    @Value("${topic:my_log_topic}")
    private   String topic;
    // the below two parameters define the range of partitions to be processed by this app
    // first partition in the Kafka Topic from which the messages have to be processed
    @Value("${firstPartition:0}")
    private   short firstPartition;
    // Name of the Kafka Consumer Group
    @Value("${consumerGroupName:kafka_es_indexer}")
    private   String consumerGroupName;

    @Override
    public void addMessageToBatch(byte[] inputMessage, Long offset) throws Exception {
        elasticSearchBatchService.addEventToBulkRequest(new String(inputMessage),"raw-index","raw-type",null,null);
    }

    @Override
    public boolean postToElasticSearch() throws Exception {
        return elasticSearchBatchService.postToElasticSearch();

    }

    /**
	 * @throws Exception
	 */
	public AccessLogMessageHandlerImpl() throws Exception {
		super();
		// TODO Auto-generated constructor stub
	}

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private ElasticSearchBatchService elasticSearchBatchService ;

    private static final String actualTimeZone = "Europe/London";
    private static final String expectedTimeZone = "Europe/London";
    private static final SimpleDateFormat actualFormatter =new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss") ;
    private static final SimpleDateFormat expectedFormatter =new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ") ;

    @PostConstruct
    public void init(){
        actualFormatter.setTimeZone(TimeZone.getTimeZone(actualTimeZone));
        expectedFormatter.setTimeZone(TimeZone.getTimeZone(expectedTimeZone));
    }

    @Override
    public byte[] transformMessage(byte[] inputMessage, Long offset) throws Exception {
		String outputMessageStr = convertToJson(new String(inputMessage, "UTF-8"), offset);
		return outputMessageStr.getBytes(); 		
    }

	public String convertToJson(String rawMsg, Long offset) throws Exception{				
        String[] splitMsg =  rawMsg.split("\\|");
        for(int i=0; i < splitMsg.length; i++){
            splitMsg[i] = splitMsg[i].trim();
        }

        AccessLogMapper accessLogMsgObj = new AccessLogMapper();
        accessLogMsgObj.setRawMessage(rawMsg);
        accessLogMsgObj.getKafkaMetaData().setOffset(offset);
        accessLogMsgObj.getKafkaMetaData().setTopic(topic);
        accessLogMsgObj.getKafkaMetaData().setConsumerGroupName(consumerGroupName);
        accessLogMsgObj.getKafkaMetaData().setPartition(firstPartition);
        accessLogMsgObj.setIp(splitMsg[0].trim());
        accessLogMsgObj.setProtocol(splitMsg[1].trim());

        if(splitMsg[5].toUpperCase().contains("GET")){
            accessLogMsgObj.setIp(splitMsg[3].trim());
            accessLogMsgObj.setProtocol(splitMsg[4].trim());

            accessLogMsgObj.setMethod(splitMsg[5].trim());
            accessLogMsgObj.setPayLoad(splitMsg[6].trim());
            accessLogMsgObj.setResponseCode(Integer.parseInt(splitMsg[8].trim()));
            accessLogMsgObj.setSessionID(splitMsg[9].trim());
            String [] serverAndInstance = splitMsg[9].split("\\.")[1].split("-");

            accessLogMsgObj.setServerName(serverAndInstance[0].trim());
            accessLogMsgObj.setInstance(serverAndInstance[1].trim());
            accessLogMsgObj.setServerAndInstance(serverAndInstance[0].trim() + "_" + serverAndInstance[1].trim());

            accessLogMsgObj.setHostName(splitMsg[12].split(" " )[0].trim());
            accessLogMsgObj.setResponseTime(Integer.parseInt(splitMsg[13].trim()));
            accessLogMsgObj.setUrl(splitMsg[11].trim());
            accessLogMsgObj.setAjpThreadName(splitMsg[14].trim());
            accessLogMsgObj.setSourceIpAndPort(null);

            actualFormatter.setTimeZone(TimeZone.getTimeZone(actualTimeZone));

            expectedFormatter.setTimeZone(TimeZone.getTimeZone(expectedTimeZone));
            String [] dateString = splitMsg[0].split(" " );
            Date date  = actualFormatter.parse(dateString[0].trim().replaceAll("\\[", "").trim());
            accessLogMsgObj.setTimeStamp(expectedFormatter.format(date));
        }

        if(splitMsg[5].toUpperCase().contains("POST")){
            accessLogMsgObj.setIp(splitMsg[3].trim());
            accessLogMsgObj.setProtocol(splitMsg[4].trim());

            accessLogMsgObj.setMethod(splitMsg[5].trim());
            if(!splitMsg[6].trim().isEmpty()){
                accessLogMsgObj.setPayLoad(splitMsg[6].trim());
            }
            accessLogMsgObj.setResponseCode(Integer.parseInt(splitMsg[8].trim()));
            accessLogMsgObj.setSessionID(splitMsg[9].trim());
            String[] serverAndInstance = splitMsg[9].split("\\.")[1].split("-");

            accessLogMsgObj.setServerName(serverAndInstance[0].trim());
            accessLogMsgObj.setInstance(serverAndInstance[1].trim());
            accessLogMsgObj.setServerAndInstance(serverAndInstance[0].trim() + "_" + serverAndInstance[1].trim());

            accessLogMsgObj.setHostName(splitMsg[12].trim().split(" " )[0]);
            accessLogMsgObj.setResponseTime(Integer.parseInt(splitMsg[13].trim()));
            accessLogMsgObj.setUrl(splitMsg[11].trim());
            accessLogMsgObj.setAjpThreadName(splitMsg[14].trim());
            accessLogMsgObj.setSourceIpAndPort(null);
            String [] dateString = splitMsg[0].split(" " );
            Date date = actualFormatter.parse(dateString[0].trim().replaceAll("\\[", "").trim());
            accessLogMsgObj.setTimeStamp(expectedFormatter.format(date));
        }
        return objectMapper.writeValueAsString(accessLogMsgObj);
	}


}
