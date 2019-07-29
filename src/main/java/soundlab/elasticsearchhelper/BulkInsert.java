package soundlab.elasticsearchhelper;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.Closeable;
import java.io.IOException;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class BulkInsert implements Closeable {
    RestHighLevelClient client;
    String index;
    int size;
    ElasticsearchConfig config;

    @NonFinal
    BulkRequest bulkRequest;

    private BulkInsert(RestHighLevelClient client, ElasticsearchConfig config, String index, int size) {
        this.client = client;
        this.config = config;
        this.index = index;
        this.size = size;
        this.bulkRequest = new BulkRequest();
    }

    public static BulkInsert create(RestHighLevelClient client, ElasticsearchConfig config, String index, int size) {
        return new BulkInsert(client, config, index, size);
    }

    public <T> void save(T entity) throws IOException {
        // insert to buffer
        var request = new IndexRequest(index);
        var json = JsonUtil.getJson(config.getObjectMapper(), entity);
        request.source(json, XContentType.JSON);
        bulkRequest.add(request);

        // check for buffer
        if (bulkRequest.requests().size() % size == 0) {
            flush();
        }
    }

    public void flush() throws IOException {
        var itemSize = bulkRequest.requests().size();
        if (itemSize == 0)
            return;

        var bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (bulkResponse.hasFailures()) {
            throw new IOException("Bulk error: " + bulkResponse.buildFailureMessage());
        }
        // reset
        bulkRequest = new BulkRequest();
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
