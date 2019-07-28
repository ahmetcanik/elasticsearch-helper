package elasticsearch.helper;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Getter
public class ElasticHelperClient implements Closeable {
    RestHighLevelClient elasticClient;
    ElasticHelperConfig config;

    public ElasticHelperClient() {
        this.elasticClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        this.config = new ElasticHelperConfig();
    }

    public ElasticHelperClient(ElasticHelperConfig config, String hostname, int port) {
        this.elasticClient = new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, port, "http")));
        this.config = config;
    }

    private static List<String> toStringList(Text[] fragments) {
        return Arrays.stream(fragments).map(Text::string).collect(Collectors.toList());
    }

    private static void setSorting(ElasticQueryBuilder elasticQueryBuilder, SearchSourceBuilder searchSourceBuilder) {
        if (elasticQueryBuilder.sortFieldName() != null) {
            var sort =
                    SortBuilders.fieldSort(elasticQueryBuilder.sortFieldName()).order(elasticQueryBuilder.sortOrder());
            if (elasticQueryBuilder.nestedSortPath() != null)
                sort.setNestedSort(new NestedSortBuilder(elasticQueryBuilder.nestedSortPath()));
            searchSourceBuilder.sort(sort);
        }
    }

    public Optional<String> find(ElasticQueryBuilder elasticQueryBuilder) throws IOException {
        SearchRequest searchRequest;
        if (elasticQueryBuilder.indices() == null)
            searchRequest = new SearchRequest();
        else
            searchRequest = new SearchRequest(elasticQueryBuilder.indices());
        var searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(elasticQueryBuilder.query()).size(1);

        // aggregation
        if (elasticQueryBuilder.aggregation() != null)
            searchSourceBuilder.aggregation(elasticQueryBuilder.aggregation());

        // sorting
        setSorting(elasticQueryBuilder, searchSourceBuilder);

        searchRequest.source(searchSourceBuilder);
        var searchResponse = elasticClient.search(searchRequest, RequestOptions.DEFAULT);
        var hits = searchResponse.getHits();

        //        if (hits.getTotalHits().value == 0)
        if (hits.totalHits == 0)
            return Optional.empty();

        var json = hits.getAt(0).getSourceAsString();

        // do masking
        for (var field : elasticQueryBuilder.maskFields().keySet()) {
            json = JsonUtil.setNodeValue(config.getObjectMapper(), field, elasticQueryBuilder.maskFields().get(field),
                    json);
        }

        return Optional.of(json);
    }

    public SearchResult findAll(ElasticQueryBuilder elasticQueryBuilder) throws IOException {
        SearchRequest searchRequest;
        if (elasticQueryBuilder.indices() == null)
            searchRequest = new SearchRequest();
        else
            searchRequest = new SearchRequest(elasticQueryBuilder.indices());

        var searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(elasticQueryBuilder.query());
        if (elasticQueryBuilder.from() > -1)
            searchSourceBuilder.from(elasticQueryBuilder.from());
        if (elasticQueryBuilder.size() > 0)
            searchSourceBuilder.size(elasticQueryBuilder.size());

        if (elasticQueryBuilder.highlightFields() != null) {
            var highlightBuilder = new HighlightBuilder();
            highlightBuilder.fields().addAll(
                    Arrays.stream(elasticQueryBuilder.highlightFields()).map(HighlightBuilder.Field::new)
                            .collect(Collectors.toList()));
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

        var startTime = System.nanoTime();
        // get response
        var searchResponse = elasticClient.search(searchRequest, RequestOptions.DEFAULT);

        var from = elasticQueryBuilder.from();
        //        var totalHits = searchResponse.getHits().getTotalHits().value;
        var totalHits = searchResponse.getHits().totalHits;
        var size = totalHits > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) totalHits;
        var took = searchResponse.getTook().getMillis();

        var searchHits = searchResponse.getHits().getHits();
        List<SearchHit> hits = new ArrayList<>();

        // scrolling
        if (elasticQueryBuilder.scroll() != null) {
            var scrollId = searchResponse.getScrollId();
            while (scrollId != null && !scrollId.isEmpty() && searchHits != null && searchHits.length > 0) {
                hits.addAll(Arrays.asList(searchHits));
                var scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(elasticQueryBuilder.scroll());
                searchResponse = elasticClient.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }
        } else
            hits.addAll(Arrays.asList(searchHits));

        List<String> jsonBuilder = new ArrayList<>();

        for (var hit : hits) {
            // write id
            //            var json = JsonUtil.writeId(config.getObjectMapper(), hit.getId(), hit.getSourceAsString());
            var json = hit.getSourceAsString();

            // do masking
            for (var field : elasticQueryBuilder.maskFields().keySet()) {
                json = JsonUtil.setNodeValue(config.getObjectMapper(), field,
                        elasticQueryBuilder.maskFields().get(field), json);
            }

            // do highlighting
            for (var entry : hit.getHighlightFields().entrySet()) {
                json = JsonUtil.setNodeValue(config.getObjectMapper(), config.getSnippetFieldName(),
                        String.join("...", toStringList(entry.getValue().fragments())), json);
            }

            jsonBuilder.add(json);
        }

        String json;

        if (elasticQueryBuilder.aggregation() == null)
            json = "[" + String.join(",", jsonBuilder) + "]";
        else {
            var builder = XContentFactory.jsonBuilder();
            builder.startObject();
            searchResponse.getAggregations().toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            json = Strings.toString(builder);
        }

        return new SearchResult(json, from, size, took);
    }

    public String save(String index, String json) throws IOException {
        try {
            IndexRequest request;
            // get id if exists
            var id = JsonUtil.getId(config.getObjectMapper(), json);
            if (id.isPresent()) {
                //            var request = new IndexRequest(index);
                request = new IndexRequest(index, config.getElasticType()).id(id.get());
            } else {
                //            var request = new IndexRequest(index);
                request = new IndexRequest(index, config.getElasticType());
            }
            request.source(json, XContentType.JSON);
            var response = elasticClient.index(request, RequestOptions.DEFAULT);
            return response.getId();
        } catch (IOException ioe) {
            throw new IOException(ioe);
        }
    }

    public String save(String index, String id, String json) throws IOException {
        try {
            //            var request = new IndexRequest(index).id(id);
            var request = new IndexRequest(index, config.getElasticType()).id(id);
            request.source(json, XContentType.JSON);
            var response = elasticClient.index(request, RequestOptions.DEFAULT);
            return response.getId();
        } catch (IOException ioe) {
            throw new IOException(ioe);
        }
    }

   /* @SuppressWarnings("unchecked")
    public <T> T update( String index, T entity,
                        String... ignoreFields) throws IOException {
        var source = JsonUtil.deleteId(config.getObjectMapper(),
                config.getObjectMapper().writeValueAsString(entity));

        for (var ignoreField : ignoreFields) {
            source = JsonUtil.removeField(config.getObjectMapper(), ignoreField, source);
        }

        var request = new UpdateRequest(index, config.getElasticType(), entity.get_id())
                .doc(source, XContentType.JSON);

        request.fetchSource(true);

        var updateResponse = client.update(request, RequestOptions.DEFAULT);

        var getResult = updateResponse.getGetResult();

        return getEntity(entity, getResult);
    }*/

    public Optional<String> findById(String index, Object id) throws IOException {
        //        var request = new GetRequest(index, id);
        var request = new GetRequest(index, config.getElasticType(), id.toString());
        var response = elasticClient.get(request, RequestOptions.DEFAULT);
        if (response.isExists())
            return Optional.of(response.getSourceAsString());
        else
            return Optional.empty();
    }

    public <T> Optional<T> findById(String index, Object id, Class<T> valueType) throws IOException {
        var json = findById(index, id);
        if (json.isPresent())
            return Optional.of(config.getObjectMapper().readValue(json.get(), valueType));
        else
            return Optional.empty();
    }

    public void deleteById(String index, Object id) throws IOException {
        //        var request = new DeleteRequest(index, id);
        var request = new DeleteRequest(index, config.getElasticType(), id.toString());
        var response = elasticClient.delete(request, RequestOptions.DEFAULT);
        if (response.status() != RestStatus.OK)
            throw new IOException("Delete failed: " + response.status().name());
    }

    public <T> Optional<T> find(ElasticQueryBuilder elasticQueryBuilder, Class<T> valueType) throws IOException {
        var json = find(elasticQueryBuilder);
        if (json.isEmpty())
            return Optional.empty();
        return Optional.of(JsonUtil.getObject(json.get(), config.getObjectMapper(), valueType));
    }

    public <T> List<T> findAll(ElasticQueryBuilder elasticQueryBuilder, Class<T> valueType) throws IOException {
        var json = findAll(elasticQueryBuilder).getResult();
        return JsonUtil.getList(json, config.getObjectMapper(), valueType);
    }

    @Override
    public void close() throws IOException {
        this.elasticClient.close();
    }

    @SuppressWarnings("unchecked")
    private <T> T find(T entity, GetResult getResult) throws IOException {
        if (getResult.isExists()) {
            var json = getResult.sourceAsString();
            return (T) JsonUtil.getObject(json, config.getObjectMapper(), entity.getClass());
        } else
            return null;
    }
}
