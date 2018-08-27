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

[Sorting](#Sorting)

[Pagination](#Pagination)

[Aggregation](###Aggregation)

[Scrolling](###Scrolling)

[Highlighting](###Highlighting)

[Masking](###Masking)


### Sorting
To sort `twitter` index by the field `created_at` and order `DESC`:
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

### Pagination
To get 10 search hits starting from 20 (records 20,21,..,29):
```
try (RestHighLevelClient restHighLevelClient = ElasticHelper.buildLocalClient()) {
    MatchAllQueryBuilder query = QueryBuilders.matchAllQuery();
    ElasticQueryBuilder builder = new ElasticQueryBuilder(restHighLevelClient, query)
            .indices("twitter")
            .from(20) // .from(previousResult.getFrom() + HITS_PER_PAGE)
            .size(10) // .size(HITS_PER_PAGE);
    SearchResult result = ElasticHelper.queryMultiple(builder);
} catch (IOException e) {
    e.printStackTrace();
}
```
`SearchResult` class holds `from` and `size`, so pagination can be resumed with these values from previous searches.