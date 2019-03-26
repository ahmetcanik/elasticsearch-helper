package elasticsearch.helper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.IOException;
import java.util.List;

public class JsonUtil {

    public static final String INTERNAL_ID_COLUMN_NAME = "_id";

    /**
     * Builds object from given json
     *
     * @param json         json string
     * @param objectMapper ObjectMapper
     * @param valueType    object type to build
     * @param <T>          type of the object
     * @return Built object
     */
    public static <T> T getObject(String json, ObjectMapper objectMapper, Class<T> valueType) throws IOException {
        return objectMapper.readValue(json, valueType);
    }

    /**
     * Builds list of object from given json
     *
     * @param json         json string
     * @param objectMapper ObjectMapper
     * @param valueType    list type to build
     * @param <T>          type of the object
     * @return Built object
     */
    public static <T> List<T> getList(String json, ObjectMapper objectMapper, Class<T> valueType) throws IOException {
        TypeFactory typeFactory = objectMapper.getTypeFactory();
        return objectMapper.readValue(json, typeFactory.constructCollectionType(List.class, valueType));
    }

    public static <T extends ElasticEntity> String getJson(ObjectMapper objectMapper, T entity) throws IOException {
        String json = objectMapper.writeValueAsString(entity);
        return deleteId(objectMapper, json);
    }

    public static String writeId(ObjectMapper objectMapper, String id, String source) {
        try {
            JsonNode jsonNode = objectMapper.readTree(source);
            ((ObjectNode) jsonNode).put(INTERNAL_ID_COLUMN_NAME, id);

            return objectMapper.writeValueAsString(jsonNode);
        } catch (IOException e) {
            return source;
        }
    }

    public static String deleteId(ObjectMapper objectMapper, String source) {
        return removeField(objectMapper, INTERNAL_ID_COLUMN_NAME, source);
    }

    public static String removeField(ObjectMapper objectMapper, String fieldName, String source) {
        try {
            JsonNode jsonNode = objectMapper.readTree(source);
            ((ObjectNode) jsonNode).remove(fieldName);

            return objectMapper.writeValueAsString(jsonNode);
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
    public static String setNodeValue(ObjectMapper objectMapper, String fieldName, String value, String jsonSource) throws IOException {
        JsonNode jsonNode = objectMapper.readTree(jsonSource);

        if (jsonNode.isArray()) {
            for (JsonNode node : jsonNode) {
                ((ObjectNode) node).put(fieldName, value);
            }
        } else {
            ((ObjectNode) jsonNode).put(fieldName, value);
        }

        return objectMapper.writeValueAsString(jsonNode);

    }
}
