package soundlab.elasticsearchhelper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class JsonUtil {

    public static final String ID_FIELD_NAME = "id";

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
        var typeFactory = objectMapper.getTypeFactory();
        return objectMapper.readValue(json, typeFactory.constructCollectionType(List.class, valueType));
    }

    public static <T> String getJson(ObjectMapper objectMapper, T entity) throws IOException {
        return objectMapper.writeValueAsString(entity);
    }

    public static Optional<String> getId(ObjectMapper objectMapper, String source) {
        try {
            var jsonNode = (ObjectNode) objectMapper.readTree(source);
            var idNode = jsonNode.get(ID_FIELD_NAME);
            if (idNode == null)
                return Optional.empty();
            return Optional.of(idNode.asText());
        } catch (IOException e) {
            return Optional.empty();
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
    public static String setNodeValue(ObjectMapper objectMapper, String fieldName, String value, String jsonSource)
            throws IOException {
        var jsonNode = objectMapper.readTree(jsonSource);

        if (jsonNode.isArray()) {
            for (var node : jsonNode) {
                ((ObjectNode) node).put(fieldName, value);
            }
        } else {
            ((ObjectNode) jsonNode).put(fieldName, value);
        }

        return objectMapper.writeValueAsString(jsonNode);

    }
}
