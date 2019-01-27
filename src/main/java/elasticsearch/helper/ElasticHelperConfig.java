package elasticsearch.helper;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;

public class ElasticHelperConfig {
    private static ObjectMapper defaultMapper;
    private static ObjectMapper snakeMapper;

    private ObjectMapper jsonObjectMapper = getDefaultMapper();
    private String elasticType = "_doc";
    private String snippetFieldName = "snippet";

    static {
        defaultMapper = new ObjectMapper();
        defaultMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        defaultMapper.setVisibility(VisibilityChecker.Std.defaultInstance().withFieldVisibility(JsonAutoDetect.Visibility.ANY));

        snakeMapper = new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        snakeMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        snakeMapper.setVisibility(VisibilityChecker.Std.defaultInstance().withFieldVisibility(JsonAutoDetect.Visibility.ANY));
    }

    public static ObjectMapper getDefaultMapper() {
        return defaultMapper;
    }

    public static ObjectMapper getSnakeMapper() {
        return snakeMapper;
    }

    public ObjectMapper getJsonObjectMapper() {
        return jsonObjectMapper;
    }

    public void setJsonObjectMapper(ObjectMapper jsonObjectMapper) {
        this.jsonObjectMapper = jsonObjectMapper;
    }

    public String getElasticType() {
        return elasticType;
    }

    public void setElasticType(String elasticType) {
        this.elasticType = elasticType;
    }

    public String getSnippetFieldName() {
        return snippetFieldName;
    }

    public void setSnippetFieldName(String snippetFieldName) {
        this.snippetFieldName = snippetFieldName;
    }
}
