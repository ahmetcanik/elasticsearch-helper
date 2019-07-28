package elasticsearch.helper;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;

import java.io.Serializable;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ElasticsearchHelperRepository<T, ID extends Serializable> {
    ElasticHelperClient elasticHelperClient;

    public ElasticsearchHelperRepository() {
        elasticHelperClient = new ElasticHelperClient();
    }

    public ElasticsearchHelperRepository(ElasticHelperConfig config, String hostname, int port) {
        elasticHelperClient = new ElasticHelperClient(config, hostname, port);
    }
}
