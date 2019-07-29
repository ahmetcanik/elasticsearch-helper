package soundlab.elasticsearchhelper;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Getter
public class ElasticsearchConfig {
    @Getter
    private static ObjectMapper defaultMapper;
    @Getter
    private static ObjectMapper snakeMapper;

    static {
        defaultMapper = new ObjectMapper();
        defaultMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        defaultMapper.setVisibility(VisibilityChecker
                .Std.defaultInstance().withFieldVisibility(JsonAutoDetect.Visibility.ANY));

        snakeMapper = new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        snakeMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        snakeMapper.setVisibility(VisibilityChecker
                .Std.defaultInstance().withFieldVisibility(JsonAutoDetect.Visibility.ANY));
    }

    String elasticType;
    String snippetFieldName;
    ObjectMapper objectMapper;

    public ElasticsearchConfig() {
        this("_doc", "snippet", getDefaultMapper());
    }

    public ElasticsearchConfig(String elasticType, String snippetFieldName, ObjectMapper objectMapper) {
        this.elasticType = elasticType;
        this.snippetFieldName = snippetFieldName;
        this.objectMapper = objectMapper;
    }

    public String getElasticType() {
        return elasticType;
    }

    public String getSnippetFieldName() {
        return snippetFieldName;
    }
}
