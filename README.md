# elasticsearch-helper
elasticsearch-helper makes life easier when using [Elasticsearch Java High Level REST Client](https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high.html).

With the help of builder design pattern, `ElasticQueryBuilder` builds the query with almost all possible options.

General code snippet is 
```
try (RestHighLevelClient restHighLevelClient = ElasticHelper.buildLocalClient()) {
    MatchAllQueryBuilder query = QueryBuilders.matchAllQuery();
    ElasticQueryBuilder builder = new ElasticQueryBuilder(restHighLevelClient, query)
        .indices("twitter");
    String json = ElasticHelper.querySingle(builder);
} catch (IOException e) {
    e.printStackTrace();
}
```
which return one document of the `twitter` index.

Helper capabilities:

[1.Sorting](###1.Sorting)

[2.Pagination](###2.Pagination)

[3.Aggregation](###3.Aggregation)

[4.Scrolling](###4.Scrolling)

[5.Highlighting](###5.Highlighting)

[6.Masking](###6.Masking)


###1.Sorting
```
try (RestHighLevelClient restHighLevelClient = ElasticHelper.buildLocalClient()) {
    MatchAllQueryBuilder query = QueryBuilders.matchAllQuery();
    ElasticQueryBuilder builder = new ElasticQueryBuilder(restHighLevelClient, query)
            .indices("twitter")
            .sortFieldName("created_at")
            .sortOrder(SortOrder.DESC);
    String json = ElasticHelper.querySingle(builder);
} catch (IOException e) {
    e.printStackTrace();
}
```
