package com.github.ahmetcanik.elasticsearchhelper;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;

import java.io.Serializable;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
class ElasticsearchRepository<T, ID extends Serializable> {
    ElasticsearchClient elasticsearchClient;

    public ElasticsearchRepository() {
        elasticsearchClient = new ElasticsearchClient();
    }

    public ElasticsearchRepository(ElasticsearchConfig config, String hostname, int port) {
        elasticsearchClient = new ElasticsearchClient(config, hostname, port);
    }
}
