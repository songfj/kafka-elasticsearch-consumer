package org.elasticsearch.kafka.indexer.service;

import java.util.HashMap;

/**
 * Created by dhyan on 1/28/16.
 */
public interface IIndexHandler {
    public String getIndexName (HashMap<String, Object> indexLookupProperties);
    public String getIndexType (HashMap<String, Object> indexLookupProperties);
}
