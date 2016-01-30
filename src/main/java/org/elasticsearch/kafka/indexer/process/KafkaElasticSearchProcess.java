package org.elasticsearch.kafka.indexer.process;

import org.elasticsearch.kafka.indexer.service.JobManagerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by dhyan on 1/28/16.
 */
public class KafkaElasticSearchProcess {
    private static final Logger logger = LoggerFactory.getLogger(KafkaElasticSearchProcess.class);
    public static void main(String[] args) throws Exception {
        logger.info("Starting KafkaElasticSearchProcess  ");
        ClassPathXmlApplicationContext indexerContext = new ClassPathXmlApplicationContext("spring/kafka-es-context.xml");
        indexerContext.registerShutdownHook();
        indexerContext.getBean(JobManagerService.class).processAllThreads();
        logger.info("Starting KafkaElasticSearchProcess  ");

    }
}
