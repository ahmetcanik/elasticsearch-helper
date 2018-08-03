package elastic.helper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;

public class JsonUtil {
	private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

	private static final ObjectMapper mapper = new ObjectMapper();
	private static final ObjectMapper snakeMapper = new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);


	public static ObjectMapper getMapper() {
		return mapper;
	}

	public static ObjectMapper getSnakeMapper() {
		return snakeMapper;
	}

	/**
	 * Builds object from given json
	 *
	 * @param json      json string
	 * @param valueType object type to build
	 * @param <T>       type of the object
	 * @return Built object, null if building failed
	 */
	public static <T> T getObject(String json, Class<T> valueType) {
		try {
			return JsonUtil.getMapper().readValue(json, valueType);
		} catch (IOException e) {
			logger.error("Json deserialization error for: " + json, e);
			return null;
		}
	}

	/**
	 * Builds list of object from given json
	 *
	 * @param json      json string
	 * @param valueType list type to build
	 * @param <T>       type of the object
	 * @return Built object, null if building failed
	 */
	public static <T> List<T> getList(String json, Class<T> valueType) {
		TypeFactory typeFactory = JsonUtil.getMapper().getTypeFactory();
		try {
			return JsonUtil.getMapper().readValue(json, typeFactory.constructCollectionType(List.class, valueType));
		} catch (IOException e) {
			logger.error("Json deserialization error for: " + json, e);
			return null;
		}
	}
}
