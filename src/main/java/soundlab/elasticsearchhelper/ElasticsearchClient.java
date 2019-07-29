package soundlab.elasticsearchhelper;

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
import org.elasticsearch.index.query.QueryBuilders;
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
public class ElasticsearchClient implements Closeable {
    RestHighLevelClient elasticClient;
    ElasticsearchConfig config;

    public ElasticsearchClient() {
        this.elasticClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        this.config = new ElasticsearchConfig();
    }

    public ElasticsearchClient(ElasticsearchConfig config, String hostname, int port) {
        this.elasticClient = new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, port, "http")));
        this.config = config;
    }

    private static List<String> toStringList(Text[] fragments) {
        return Arrays.stream(fragments).map(Text::string).collect(Collectors.toList());
    }

    private static void setSorting(ElasticsearchQuery elasticHelperQuery, SearchSourceBuilder searchSourceBuilder) {
        if (elasticHelperQuery.getSortFieldName() != null) {
            var sort =
                    SortBuilders.fieldSort(elasticHelperQuery.getSortFieldName())
                            .order(elasticHelperQuery.getSortOrder());
            if (elasticHelperQuery.getNestedSortPath() != null)
                sort.setNestedSort(new NestedSortBuilder(elasticHelperQuery.getNestedSortPath()));
            searchSourceBuilder.sort(sort);
        }
    }

    public Optional<String> querySingle(ElasticsearchQuery elasticHelperQuery) throws IOException {
        if (elasticHelperQuery.getQuery() == null)
            throw new IllegalArgumentException("Query is not set in elasticHelperQuery");
        SearchRequest searchRequest;
        if (elasticHelperQuery.getIndices() == null)
            searchRequest = new SearchRequest();
        else
            searchRequest = new SearchRequest(elasticHelperQuery.getIndices().toArray(new String[0]));
        var searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(elasticHelperQuery.getQuery()).size(1);

        // aggregation
        if (elasticHelperQuery.getAggregation() != null)
            searchSourceBuilder.aggregation(elasticHelperQuery.getAggregation());

        // sorting
        setSorting(elasticHelperQuery, searchSourceBuilder);

        searchRequest.source(searchSourceBuilder);
        var searchResponse = elasticClient.search(searchRequest, RequestOptions.DEFAULT);
        var hits = searchResponse.getHits();

        //        if (hits.getTotalHits().value == 0)
        if (hits.totalHits == 0)
            return Optional.empty();

        var json = hits.getAt(0).getSourceAsString();

        // do masking
        for (var field : elasticHelperQuery.getMaskFields().keySet()) {
            json = JsonUtil.setNodeValue(config.getObjectMapper(), field, elasticHelperQuery.getMaskFields().get(field),
                    json);
        }

        return Optional.of(json);
    }

    public SearchResult queryAll(ElasticsearchQuery elasticHelperQuery) throws IOException {
        if (elasticHelperQuery.getQuery() == null)
            throw new IllegalArgumentException("Query is not set in elasticHelperQuery");
        SearchRequest searchRequest;
        if (elasticHelperQuery.getIndices() == null)
            searchRequest = new SearchRequest();
        else
            searchRequest = new SearchRequest(elasticHelperQuery.getIndices().toArray(new String[0]));

        var searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(elasticHelperQuery.getQuery());
        if (elasticHelperQuery.getFrom() > -1)
            searchSourceBuilder.from(elasticHelperQuery.getFrom());
        if (elasticHelperQuery.getSize() > 0)
            searchSourceBuilder.size(elasticHelperQuery.getSize());

        if (elasticHelperQuery.getHighlightFields() != null) {
            var highlightBuilder = new HighlightBuilder();
            highlightBuilder.fields().addAll(
                    elasticHelperQuery.getHighlightFields().stream().map(HighlightBuilder.Field::new)
                            .collect(Collectors.toList()));
            searchSourceBuilder.highlighter(highlightBuilder);
        }

        // aggregation
        if (elasticHelperQuery.getAggregation() != null) {
            searchSourceBuilder.aggregation(elasticHelperQuery.getAggregation());
            searchSourceBuilder.size(0); // no terms, just aggregations
        }

        // sorting
        setSorting(elasticHelperQuery, searchSourceBuilder);
        searchRequest.source(searchSourceBuilder);

        // source filtering
        if (elasticHelperQuery.getIncludeFields() != null || elasticHelperQuery.getExcludeFields() != null)
            searchSourceBuilder
                    .fetchSource(elasticHelperQuery.getIncludeFields().toArray(new String[0]),
                            elasticHelperQuery.getExcludeFields().toArray(new String[0]));

        // scrolling
        if (elasticHelperQuery.getScroll() != null) {
            searchRequest.scroll(elasticHelperQuery.getScroll());
        }

        // get response
        var searchResponse = elasticClient.search(searchRequest, RequestOptions.DEFAULT);

        var from = elasticHelperQuery.getFrom();
        //        var totalHits = searchResponse.getHits().getTotalHits().value;
        var totalHits = searchResponse.getHits().totalHits;
        var size = totalHits > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) totalHits;
        var took = searchResponse.getTook().getMillis();

        var searchHits = searchResponse.getHits().getHits();
        List<SearchHit> hits = new ArrayList<>();

        // scrolling
        if (elasticHelperQuery.getScroll() != null) {
            var scrollId = searchResponse.getScrollId();
            while (scrollId != null && !scrollId.isEmpty() && searchHits != null && searchHits.length > 0) {
                hits.addAll(Arrays.asList(searchHits));
                var scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(elasticHelperQuery.getScroll());
                searchResponse = elasticClient.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }
        } else
            hits.addAll(Arrays.asList(searchHits));

        List<String> jsonBuilder = new ArrayList<>();

        for (var hit : hits) {
            var json = hit.getSourceAsString();

            // do masking
            for (var field : elasticHelperQuery.getMaskFields().keySet()) {
                json = JsonUtil.setNodeValue(config.getObjectMapper(), field,
                        elasticHelperQuery.getMaskFields().get(field), json);
            }

            // do highlighting
            for (var entry : hit.getHighlightFields().entrySet()) {
                json = JsonUtil.setNodeValue(config.getObjectMapper(), config.getSnippetFieldName(),
                        String.join("...", toStringList(entry.getValue().fragments())), json);
            }

            jsonBuilder.add(json);
        }

        String json;

        if (elasticHelperQuery.getAggregation() == null)
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

    public SearchResult findAll(String index) throws IOException {
        var query = ElasticsearchQuery.builder().query(QueryBuilders.matchAllQuery()).index(index).build();
        return queryAll(query);
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

    public <T> Optional<T> querySingle(ElasticsearchQuery elasticHelperQuery, Class<T> valueType) throws IOException {
        var json = querySingle(elasticHelperQuery);
        if (json.isEmpty())
            return Optional.empty();
        return Optional.of(JsonUtil.getObject(json.get(), config.getObjectMapper(), valueType));
    }

    public <T> List<T> queryAll(ElasticsearchQuery elasticHelperQuery, Class<T> valueType) throws IOException {
        var json = queryAll(elasticHelperQuery).getResult();
        return JsonUtil.getList(json, config.getObjectMapper(), valueType);
    }

    public <T> List<T> findAll(String index, Class<T> valueType) throws IOException {
        var query = ElasticsearchQuery.builder()
                .query(QueryBuilders.matchAllQuery())
                .index(index)
                .build();
        var json = queryAll(query).getResult();
        return JsonUtil.getList(json, config.getObjectMapper(), valueType);
    }

    @Override
    public void close() throws IOException {
        this.elasticClient.close();
    }

    @SuppressWarnings("unchecked")
    private <T> T querySingle(T entity, GetResult getResult) throws IOException {
        if (getResult.isExists()) {
            var json = getResult.sourceAsString();
            return (T) JsonUtil.getObject(json, config.getObjectMapper(), entity.getClass());
        } else
            return null;
    }
}
