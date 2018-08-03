package elasticsearch.helper;

import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.HashMap;
import java.util.Map;

public class ElasticQueryBuilder {
	private final RestHighLevelClient client;
	private final QueryBuilder query;
	private AggregationBuilder aggregation = null;// null means ignore
	private String sortFieldName = null;// null means ignore
	private SortOrder sortOrder = SortOrder.ASC;
	private String nestedSortPath = null;// null means ignore
	private int from = -1;// negative means ignore
	private int size = -1;// negative means ignore
	private String[] indices = null;// null means ignore
	private String[] highlightFields = null; // null means ignore
	private Map<String, String> maskFields;
	private String scroll = null; // null means ignore

	public ElasticQueryBuilder(RestHighLevelClient client, QueryBuilder query) {
		this.client = client;
		this.query = query;
		this.maskFields = new HashMap<>();
	}

	public RestHighLevelClient client() {
		return client;
	}

	public QueryBuilder query() {
		return query;
	}

	public AggregationBuilder aggregation() {
		return aggregation;
	}

	public String sortFieldName() {
		return sortFieldName;
	}

	public SortOrder sortOrder() {
		return sortOrder;
	}

	public String nestedSortPath() {
		return nestedSortPath;
	}

	public int from() {
		return from;
	}

	public int size() {
		return size;
	}

	public String[] indices() {
		return indices;
	}

	public String[] highlightFields() {
		return highlightFields;
	}

	public Map<String, String> maskFields() {
		return maskFields;
	}

	public String scroll() {
		return scroll;
	}

	public ElasticQueryBuilder aggregation(AggregationBuilder aggregation) {
		this.aggregation = aggregation;
		return this;
	}

	public ElasticQueryBuilder sortFieldName(String sortFieldName) {
		this.sortFieldName = sortFieldName;
		return this;
	}

	public ElasticQueryBuilder sortOrder(SortOrder sortOrder) {
		this.sortOrder = sortOrder;
		return this;
	}

	public ElasticQueryBuilder nestedSortPath(String nestedSortPath) {
		this.nestedSortPath = nestedSortPath;
		return this;
	}

	public ElasticQueryBuilder from(int from) {
		this.from = from;
		return this;
	}

	public ElasticQueryBuilder size(int size) {
		this.size = size;
		return this;
	}

	public ElasticQueryBuilder indices(String... indices) {
		this.indices = indices;
		return this;
	}

	public ElasticQueryBuilder highlightFields(String... highlightFields) {
		this.highlightFields = highlightFields;
		return this;
	}

	public ElasticQueryBuilder maskFields(Map<String, String> maskFields) {
		this.maskFields = maskFields;
		return this;
	}

	public ElasticQueryBuilder maskField(String field, String mask) {
		this.maskFields.put(field, mask);

		return this;
	}

	public ElasticQueryBuilder scroll(String scroll) {
		this.scroll = scroll;
		return this;
	}
}
