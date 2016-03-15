package org.elasticsearch.kafka.indexer.service;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * Created by dhyan on 8/31/15.
 */
@Service
public class ElasticSearchClientService {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClientService.class);
    public static final String CLUSTER_NAME = "cluster.name";

    @Value("${esClusterName:KafkaESCluster}")
    private String esClusterName;

    @Value("#{'${esHostPortList:localhost:9300}'.split(',')}")
    private List<String> esHostPortList;

    private TransportClient elasticSearchClient;

    private boolean isInitialized = false;

    @PostConstruct
    public void init() {
        // connect to elasticsearch cluster
        Settings settings = ImmutableSettings.settingsBuilder().put(CLUSTER_NAME, esClusterName).build();

        //String[] esHostPortList = esHostPort.trim().split(",");
        // TODO add validation of host:port syntax - to avoid Runtime exceptions
        try {

            elasticSearchClient = new TransportClient(settings);
            for (String eachHostPort : esHostPortList) {
                logger.info("adding [{}] to TransportClient ... ", eachHostPort);
                elasticSearchClient.addTransportAddress(new InetSocketTransportAddress(eachHostPort.split(":")[0].trim(),
                        Integer.parseInt(eachHostPort.split(":")[1].trim())));
            }
            isInitialized = true;
            logger.info("ElasticSearch Client created and intialized OK");
        } catch (Exception e) {
            logger.error("Exception when trying to connect and create ElasticSearch Client. Throwing the error. Error Message is::"+ e.getMessage());
            throw e;
        }

    }

    public TransportClient getElasticSearchClient() {
        if (!isInitialized) {
            init();
        }
        return elasticSearchClient;
    }

}
