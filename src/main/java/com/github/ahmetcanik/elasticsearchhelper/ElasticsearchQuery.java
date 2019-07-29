package com.github.ahmetcanik.elasticsearchhelper;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.experimental.FieldDefaults;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;
import java.util.Map;

@FieldDefaults(level = AccessLevel.PRIVATE)
@Builder
@Getter
public class ElasticsearchQuery {
    QueryBuilder query;
    AggregationBuilder aggregation;// null means ignore
    String sortFieldName;// null means ignore
    @Builder.Default
    SortOrder sortOrder = SortOrder.ASC;
    String nestedSortPath;// null means ignore
    @Builder.Default
    int from = -1;// negative means ignore
    @Builder.Default
    int size = -1;// negative means ignore
    @Singular
    List<String> indices;// null means ignore
    @Singular
    List<String> highlightFields; // null means ignore
    @Singular
    Map<String, String> maskFields;
    String scroll; // null means ignore
    @Singular
    List<String> includeFields; // null means ignore
    @Singular
    List<String> excludeFields; // null means ignore
}
