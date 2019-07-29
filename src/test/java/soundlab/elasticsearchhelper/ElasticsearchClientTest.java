package soundlab.elasticsearchhelper;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ElasticsearchClientTest {
    private static final String INDEX_NAME = "test_entity";
    private static ObjectMapper mapper = new ObjectMapper();
    private static boolean setUpIsDone = true;

    @BeforeClass
    public static void setUp() throws IOException, InterruptedException {
        if (setUpIsDone) {
            return;
        }

        var elasticHelper = new ElasticsearchClient();
        for (var i = 1; i < 101; i++) {
            var entity = new TestEntity(i, "test" + i);
            var json = JsonUtil.getJson(mapper, entity);
            var id = elasticHelper.save(INDEX_NAME, json);
            System.out.printf("Entity saved with id: %s%n", id);
        }

        Thread.sleep(1000);
        setUpIsDone = true;
    }

    @Test
    public void find() throws IOException {
        var elasticHelper = new ElasticsearchClient();
        var id = 46;
        var query = QueryBuilders.termQuery("name", "test" + id);
        var builder = ElasticsearchQuery.builder().query(query)
                .index(INDEX_NAME)
                .build();

        var result = elasticHelper.querySingle(builder);

        assert result.isPresent();

        var expected = JsonUtil.getJson(mapper, new TestEntity(id, "test" + id));

        assertEquals(mapper.readTree(expected), mapper.readTree(result.get()));
    }

    @Test
    public void findAll() throws IOException {
        var elasticHelper = new ElasticsearchClient();
        var query = QueryBuilders.prefixQuery("name", "test");
        var builder = ElasticsearchQuery.builder().query(query)
                .index(INDEX_NAME)
                .size(1000)
                .sortFieldName("id")
                .build();

        var result = elasticHelper.queryAll(builder);

        var expectedList = new ArrayList<TestEntity>();
        for (var i = 1; i < 101; i++) {
            var entity = new TestEntity(i, "test" + i);
            expectedList.add(entity);
        }

        var expected = JsonUtil.getJson(mapper, expectedList);

        assertEquals(mapper.readTree(expected), mapper.readTree(result.getResult()));
    }

    @Test
    public void findById() throws IOException {
        var elasticHelper = new ElasticsearchClient();
        var id = 46;

        var expected = JsonUtil.getJson(mapper, new TestEntity(id, "test" + id));
        var actual = elasticHelper.findById(INDEX_NAME, id);

        assert actual.isPresent();

        assertEquals(mapper.readTree(expected), mapper.readTree(actual.get()));
    }

    @Test
    public void deleteById() throws IOException {
        var elasticHelper = new ElasticsearchClient();
        var id = 101;

        // first insert new record
        var entity = new TestEntity(id, "test" + id);
        var json = JsonUtil.getJson(mapper, entity);
        elasticHelper.save(INDEX_NAME, json);

        // check for new record
        var testRecord = elasticHelper.findById(INDEX_NAME, id);
        assert testRecord.isPresent();

        // now delete test record
        elasticHelper.deleteById(INDEX_NAME, id);

        // check if test record is deleted
        testRecord = elasticHelper.findById(INDEX_NAME, id);
        assert testRecord.isEmpty();
    }

    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    @Data
    @NoArgsConstructor(access = AccessLevel.PRIVATE, force = true)
    @AllArgsConstructor
    private static class TestEntity {
        long id;
        String name;
    }
}
