package elasticsearch.helper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;

public class BulkInserter implements Closeable {
    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    private final RestHighLevelClient client;
    private final String index;
    private final int size;
    private BulkRequest bulkRequest;
    private ElasticHelperConfig config;

    private BulkInserter(RestHighLevelClient client, ElasticHelperConfig config, String index, int size) {
        this.client = client;
        this.config = config;
        this.index = index;
        this.size = size;
        this.bulkRequest = new BulkRequest();
    }

    public static BulkInserter create(RestHighLevelClient client, ElasticHelperConfig config, String index, int size) {
        return new BulkInserter(client, config, index, size);
    }

    public <T extends ElasticEntity> void insert(T entity) throws IOException {
        // insert to buffer
        IndexRequest request = new IndexRequest(index, config.getElasticType());
        String json = JsonUtil.deleteId(config.getJsonObjectMapper(), config.getJsonObjectMapper().writeValueAsString(entity));
        request.source(json, XContentType.JSON);
        bulkRequest.add(request);

        // check for buffer
        if (bulkRequest.requests().size() % size == 0) {
            flush();
        }
    }

    public void flush() throws IOException {
        int itemSize = bulkRequest.requests().size();
        if (itemSize == 0)
            return;

        BulkResponse bulkResponse = client.bulk(bulkRequest);
        if (bulkResponse.hasFailures()) {
            throw new IOException("Bulk error: " + bulkResponse.buildFailureMessage());
        }
        // reset
        bulkRequest = new BulkRequest();

        logger.debug(String.format("%d items bulk inserted to %s", itemSize, index));
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
