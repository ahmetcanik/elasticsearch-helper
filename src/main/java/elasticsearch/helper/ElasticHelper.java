package elasticsearch.helper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
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

	public static RestHighLevelClient buildLocalClient() {
		return new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
	}

	public static RestHighLevelClient buildRemoteClient(String hostname, int port) {
		return new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, port, "http")));
	}

	public static String querySingle(ElasticQueryBuilder elasticQueryBuilder) throws IOException {
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

		String json = writeId(hits.getAt(0).getId(), hits.getAt(0).getSourceAsString());

		// do masking
		for (String field : elasticQueryBuilder.maskFields().keySet()) {
			json = setNodeValue(field, elasticQueryBuilder.maskFields().get(field), json);
		}

		return json;
	}

	public static SearchResult queryMultiple(ElasticQueryBuilder elasticQueryBuilder) throws IOException {
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

		// scrolling
		if (elasticQueryBuilder.scroll() != null) {
			searchRequest.scroll(elasticQueryBuilder.scroll());
		}

		// get response
		SearchResponse searchResponse = elasticQueryBuilder.client().search(searchRequest);

		int from = elasticQueryBuilder.from();
		int size = searchResponse.getHits().totalHits > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) searchResponse.getHits().totalHits;

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
			String json = writeId(hit.getId(), hit.getSourceAsString());

			// do masking
			for (String field : elasticQueryBuilder.maskFields().keySet()) {
				json = setNodeValue(field, elasticQueryBuilder.maskFields().get(field), json);
			}

			// do highlighting
			for (Map.Entry<String, HighlightField> entry : hit.getHighlightFields().entrySet()) {
				json = setNodeValue("snippet", String.join("...", toStringList(entry.getValue().fragments())), json);
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

		return new SearchResult(json, from, size);
	}

	public static <T extends ElasticEntity> void insert(RestHighLevelClient client, String index, T entity) throws IOException {
		try {
			String json = deleteId(JsonUtil.getSnakeMapper().writeValueAsString(entity));
			logger.info(json);
			IndexRequest request = new IndexRequest(index, "_doc");
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
	public static <T extends ElasticEntity> T update(RestHighLevelClient client, String index, T entity, String... ignoreFields) throws IOException {
		String source = deleteId(JsonUtil.getSnakeMapper().writeValueAsString(entity));

		for (String ignoreField : ignoreFields) {
			source = removeField(ignoreField, source);
		}

		UpdateRequest request = new UpdateRequest(index, "_doc", entity.getId())
				.doc(source, XContentType.JSON);

		request.fetchSource(true);

		UpdateResponse updateResponse = client.update(request);

		GetResult getResult = updateResponse.getGetResult();

		return getEntity(entity, getResult);
	}

	@SuppressWarnings("unchecked")
	public static <T extends ElasticEntity> T incrementCounter(RestHighLevelClient client, String index, String counterFieldName, T entity) throws IOException {
		UpdateRequest request = new UpdateRequest(index, "_doc", entity.getId());

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
	private static <T extends ElasticEntity> T getEntity(T entity, GetResult getResult) throws IOException {
		if (getResult.isExists()) {
			String json = getResult.sourceAsString();
			json = writeId(getResult.getId(), json);
			return (T) readValue(json, entity.getClass());
		} else
			return null;
	}

	public static String getById(RestHighLevelClient client, String index, String id) throws IOException {
		GetRequest request = new GetRequest(index, "_doc", id);
		GetResponse response = client.get(request);
		String json = response.getSourceAsString();
		return writeId(id, json);
	}

	public static <T> T getById(RestHighLevelClient client, String index, String id, Class<T> valueType) throws IOException {
		String json = getById(client, index, id);
		return JsonUtil.getSnakeMapper().readValue(json, valueType);
	}

	public static void delete(RestHighLevelClient client, String index, String id) throws IOException {
		DeleteRequest request = new DeleteRequest(index, "_doc", id);
		DeleteResponse response = client.delete(request);
		if (response.status() != RestStatus.OK)
			throw new IOException("Delete failed: " + response.status().name());
	}

	private static List<String> toStringList(Text[] fragments) {
		return Arrays.stream(fragments).map(text -> text.string()).collect(Collectors.toList());
	}

	public static <T> T readValue(String json, Class<T> valueType) throws IOException {
		return JsonUtil.getSnakeMapper().readValue(json, valueType);
	}

	public static <T> T getSingleValue(ElasticQueryBuilder elasticQueryBuilder, Class<T> valueType) throws IOException {
		String json = querySingle(elasticQueryBuilder);
		return json == null ? null : readValue(json, valueType);
	}

	public static <T> List<T> getValueList(ElasticQueryBuilder elasticQueryBuilder, Class<T> valueType) throws IOException {
		String json = queryMultiple(elasticQueryBuilder).getResult();
		TypeFactory typeFactory = JsonUtil.getSnakeMapper().getTypeFactory();
		return JsonUtil.getSnakeMapper().readValue(json, typeFactory.constructCollectionType(List.class, valueType));
	}

	public static <T extends ElasticEntity> String getJson(T entity) throws IOException {
		String json = JsonUtil.getSnakeMapper().writeValueAsString(entity);
		return deleteId(json);
	}

	private static String writeId(String id, String source) {
		try {
			JsonNode jsonNode = JsonUtil.getSnakeMapper().readTree(source);
			((ObjectNode) jsonNode).put("id", id);

			return JsonUtil.getSnakeMapper().writeValueAsString(jsonNode);
		} catch (IOException e) {
			return source;
		}
	}

	static String deleteId(String source) {
		try {
			JsonNode jsonNode = JsonUtil.getSnakeMapper().readTree(source);
			((ObjectNode) jsonNode).remove("id");

			return JsonUtil.getSnakeMapper().writeValueAsString(jsonNode);
		} catch (IOException e) {
			return source;
		}
	}

	private static String removeField(String fieldName, String source) {
		try {
			JsonNode jsonNode = JsonUtil.getSnakeMapper().readTree(source);
			((ObjectNode) jsonNode).remove(fieldName);

			return JsonUtil.getSnakeMapper().writeValueAsString(jsonNode);
		} catch (IOException e) {
			return source;
		}
	}

	/**
	 * Set given field value to given JSON
	 *
	 * @param fieldName  field name to be set
	 * @param value      new value for field
	 * @param jsonSource original JSON
	 * @return new JSON string
	 */
	private static String setNodeValue(String fieldName, String value, String jsonSource) {
		try {
			JsonNode jsonNode = JsonUtil.getSnakeMapper().readTree(jsonSource);

			if (jsonNode.isArray()) {
				for (JsonNode node : jsonNode) {
					((ObjectNode) node).put(fieldName, value);
				}
			} else {
				((ObjectNode) jsonNode).put(fieldName, value);
			}

			return JsonUtil.getSnakeMapper().writeValueAsString(jsonNode);
		} catch (IOException e) {
			return jsonSource;
		}
	}

	private static void setSorting(ElasticQueryBuilder elasticQueryBuilder, SearchSourceBuilder searchSourceBuilder) {
		if (elasticQueryBuilder.sortFieldName() != null) {
			FieldSortBuilder sort = SortBuilders.fieldSort(elasticQueryBuilder.sortFieldName()).order(elasticQueryBuilder.sortOrder());
			if (elasticQueryBuilder.nestedSortPath() != null)
				sort.setNestedSort(new NestedSortBuilder(elasticQueryBuilder.nestedSortPath()));
			searchSourceBuilder.sort(sort);
		}
	}
}
