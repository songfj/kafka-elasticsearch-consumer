package org.elasticsearch.kafka.indexer.service.impl;

import org.elasticsearch.kafka.indexer.service.IIndexHandler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;

/**
 * Created by dhyan on 1/29/16.
 */
public class BasicIndexHandler implements IIndexHandler {
    @Value("${indexName:test_index}")
    private String indexName;
    @Value("${esIndexType:test_index_type}")
    private String indexType;

    @Override
    public String getIndexName(HashMap<String, Object> indexLookupProperties) {
        return indexName;
    }

    @Override
    public String getIndexType(HashMap<String, Object> indexLookupProperties) {
        return indexType;
    }
}
