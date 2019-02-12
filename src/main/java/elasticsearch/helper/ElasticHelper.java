package elasticsearch.helper;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ElasticHelper {
    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());
    RestHighLevelClient client;
    ElasticHelperConfig config;

    public ElasticHelper(RestHighLevelClient client, ElasticHelperConfig config) {
        this.client = client;
        this.config = config;
    }

    public static RestHighLevelClient buildLocalClient() {
        return new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
    }

    public static RestHighLevelClient buildRemoteClient(String hostname, int port) {
        return new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, port, "http")));
    }

    private static List<String> toStringList(Text[] fragments) {
        return Arrays.stream(fragments).map(Text::string).collect(Collectors.toList());
    }

    private static void setSorting(ElasticQueryBuilder elasticQueryBuilder, SearchSourceBuilder searchSourceBuilder) {
        if (elasticQueryBuilder.sortFieldName() != null) {
            FieldSortBuilder sort = SortBuilders.fieldSort(elasticQueryBuilder.sortFieldName()).order(elasticQueryBuilder.sortOrder());
            if (elasticQueryBuilder.nestedSortPath() != null)
                sort.setNestedSort(new NestedSortBuilder(elasticQueryBuilder.nestedSortPath()));
            searchSourceBuilder.sort(sort);
        }
    }

    public String querySingle(ElasticQueryBuilder elasticQueryBuilder) throws IOException {
        SearchRequest searchRequest;
        if (elasticQueryBuilder.indices() == null)
            searchRequest = new SearchRequest();
        else
            searchRequest = new SearchRequest(elasticQueryBuilder.indices());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(elasticQueryBuilder.query()).size(1);

        // aggregation
        if (elasticQueryBuilder.aggregation() != null)
            searchSourceBuilder.aggregation(elasticQueryBuilder.aggregation());

        // sorting
        setSorting(elasticQueryBuilder, searchSourceBuilder);

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = elasticQueryBuilder.client().search(searchRequest);
        SearchHits hits = searchResponse.getHits();

        if (hits.totalHits == 0)
            return null;

        String json = JsonUtil.writeId(config.getJsonObjectMapper(), hits.getAt(0).getId(), hits.getAt(0).getSourceAsString());

        // do masking
        for (String field : elasticQueryBuilder.maskFields().keySet()) {
            json = JsonUtil.setNodeValue(config.getJsonObjectMapper(), field, elasticQueryBuilder.maskFields().get(field), json);
        }

        return json;
    }

    public SearchResult queryMultiple(ElasticQueryBuilder elasticQueryBuilder) throws IOException {
        SearchRequest searchRequest;
        if (elasticQueryBuilder.indices() == null)
            searchRequest = new SearchRequest();
        else
            searchRequest = new SearchRequest(elasticQueryBuilder.indices());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(elasticQueryBuilder.query());
        if (elasticQueryBuilder.from() > -1)
            searchSourceBuilder = searchSourceBuilder.from(elasticQueryBuilder.from());
        if (elasticQueryBuilder.size() > 0)
            searchSourceBuilder = searchSourceBuilder.size(elasticQueryBuilder.size());

        if (elasticQueryBuilder.highlightFields() != null) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            highlightBuilder.fields().addAll(
                    Arrays.stream(elasticQueryBuilder.highlightFields()).map(HighlightBuilder.Field::new).collect(Collectors.toList()));
            searchSourceBuilder.highlighter(highlightBuilder);
        }

        // aggregation
        if (elasticQueryBuilder.aggregation() != null) {
            searchSourceBuilder.aggregation(elasticQueryBuilder.aggregation());
            searchSourceBuilder.size(0); // no terms, just aggregations
        }

        // sorting
        setSorting(elasticQueryBuilder, searchSourceBuilder);
        searchRequest.source(searchSourceBuilder);

        // source filtering
        if (elasticQueryBuilder.includeFields() != null || elasticQueryBuilder.excludeFields() != null)
            searchSourceBuilder.fetchSource(elasticQueryBuilder.includeFields(), elasticQueryBuilder.excludeFields());

        // scrolling
        if (elasticQueryBuilder.scroll() != null) {
            searchRequest.scroll(elasticQueryBuilder.scroll());
        }

        long startTime = System.nanoTime();
        // get response
        SearchResponse searchResponse = elasticQueryBuilder.client().search(searchRequest);
        logger.warn(String.format("ES search time %d ms", (System.nanoTime() - startTime) / 1000000));

        int from = elasticQueryBuilder.from();
        int size = searchResponse.getHits().totalHits > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) searchResponse.getHits().totalHits;
        long took = searchResponse.getTook().getMillis();

        SearchHit[] searchHits = searchResponse.getHits().getHits();
        List<SearchHit> hits = new ArrayList<>();

        // scrolling
        if (elasticQueryBuilder.scroll() != null) {
            String scrollId = searchResponse.getScrollId();
            while (scrollId != null && !scrollId.isEmpty() && searchHits != null && searchHits.length > 0) {
                hits.addAll(Arrays.asList(searchHits));
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(elasticQueryBuilder.scroll());
                searchResponse = elasticQueryBuilder.client().searchScroll(scrollRequest);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }
        } else
            hits.addAll(Arrays.asList(searchHits));

        List<String> jsonBuilder = new ArrayList<>();

        for (SearchHit hit : hits) {
            // write id
            String json = JsonUtil.writeId(config.getJsonObjectMapper(), hit.getId(), hit.getSourceAsString());

            // do masking
            for (String field : elasticQueryBuilder.maskFields().keySet()) {
                json = JsonUtil.setNodeValue(config.getJsonObjectMapper(), field, elasticQueryBuilder.maskFields().get(field), json);
            }

            // do highlighting
            for (Map.Entry<String, HighlightField> entry : hit.getHighlightFields().entrySet()) {
                json = JsonUtil.setNodeValue(config.getJsonObjectMapper(), config.getSnippetFieldName(), String.join("...", toStringList(entry.getValue().fragments())), json);
            }

            jsonBuilder.add(json);
        }

        String json;

        if (elasticQueryBuilder.aggregation() == null)
            json = "[" + String.join(",", jsonBuilder) + "]";
        else {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            searchResponse.getAggregations().toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            json = builder.string();
        }

        return new SearchResult(json, from, size, took);
    }

    public <T extends ElasticEntity> void insert(RestHighLevelClient client, String index, T entity) throws IOException {
        try {
            String json = JsonUtil.deleteId(config.getJsonObjectMapper(), config.getJsonObjectMapper().writeValueAsString(entity));
            IndexRequest request = new IndexRequest(index, config.getElasticType());
            request.source(json, XContentType.JSON);
            IndexResponse response = client.index(request);

            // update entity id
            entity.setId(response.getId());
        } catch (IOException ioe) {
            logger.error("insert error", ioe);
            throw new IOException(ioe);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends ElasticEntity> T update(RestHighLevelClient client, String index, T entity, String... ignoreFields) throws IOException {
        String source = JsonUtil.deleteId(config.getJsonObjectMapper(), config.getJsonObjectMapper().writeValueAsString(entity));

        for (String ignoreField : ignoreFields) {
            source = JsonUtil.removeField(config.getJsonObjectMapper(), ignoreField, source);
        }

        UpdateRequest request = new UpdateRequest(index, config.getElasticType(), entity.getId())
                .doc(source, XContentType.JSON);

        request.fetchSource(true);

        UpdateResponse updateResponse = client.update(request);

        GetResult getResult = updateResponse.getGetResult();

        return getEntity(entity, getResult);
    }

    @SuppressWarnings("unchecked")
    public <T extends ElasticEntity> T incrementCounter(RestHighLevelClient client, String index, String counterFieldName, T entity) throws IOException {
        UpdateRequest request = new UpdateRequest(index, config.getElasticType(), entity.getId());

        Map<String, Object> parameters = java.util.Collections.singletonMap("count", 1);

        String script = "";
        // check for nested field
        if (counterFieldName.contains(".")) {
            String path = counterFieldName.substring(0, counterFieldName.indexOf('.'));
            script = "if (ctx._source." + path + " == null) ctx._source." + path + " = new HashMap();";
        }
        script += "ctx._source." + counterFieldName + " = ctx._source." + counterFieldName + " == null ? 1 : ctx._source." + counterFieldName + " + params.count";
        Script inline = new Script(ScriptType.INLINE, "painless", script, parameters);
        request.script(inline);
        request.fetchSource(true);

        UpdateResponse updateResponse = client.update(request);

        GetResult getResult = updateResponse.getGetResult();

        return getEntity(entity, getResult);
    }

    @SuppressWarnings("unchecked")
    private <T extends ElasticEntity> T getEntity(T entity, GetResult getResult) throws IOException {
        if (getResult.isExists()) {
            String json = getResult.sourceAsString();
            json = JsonUtil.writeId(config.getJsonObjectMapper(), getResult.getId(), json);
            return (T) JsonUtil.getObject(json, config.getJsonObjectMapper(), entity.getClass());
        } else
            return null;
    }

    public String getById(RestHighLevelClient client, String index, String id) throws IOException {
        GetRequest request = new GetRequest(index, config.getElasticType(), id);
        GetResponse response = client.get(request);
        String json = response.getSourceAsString();
        return JsonUtil.writeId(config.getJsonObjectMapper(), id, json);
    }

    public <T> T getById(RestHighLevelClient client, String index, String id, Class<T> valueType) throws IOException {
        String json = getById(client, index, id);
        return config.getJsonObjectMapper().readValue(json, valueType);
    }

    public void delete(RestHighLevelClient client, String index, String id) throws IOException {
        DeleteRequest request = new DeleteRequest(index, config.getElasticType(), id);
        DeleteResponse response = client.delete(request);
        if (response.status() != RestStatus.OK)
            throw new IOException("Delete failed: " + response.status().name());
    }

    public <T> T getSingleValue(ElasticQueryBuilder elasticQueryBuilder, Class<T> valueType) throws IOException {
        String json = querySingle(elasticQueryBuilder);
        return json == null ? null : JsonUtil.getObject(json, config.getJsonObjectMapper(), valueType);
    }

    public <T> List<T> getValueList(ElasticQueryBuilder elasticQueryBuilder, Class<T> valueType) throws IOException {
        String json = queryMultiple(elasticQueryBuilder).getResult();
        return JsonUtil.getList(json, config.getJsonObjectMapper(), valueType);
    }
}
