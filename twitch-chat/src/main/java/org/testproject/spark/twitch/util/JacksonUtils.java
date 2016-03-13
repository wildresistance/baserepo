package org.testproject.spark.twitch.util;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.NullNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by akurilyonok
 */
public class JacksonUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses json and finds all the particular values from the list of entities
     * @param json - raw json
     * @param arrayNodeName - the name of subnode
     * @param propertyName - name of the property to retrieve
     * @return list of property values
     */
    public static List<String> asList(String json, String arrayNodeName, String propertyName) {
        try {
            JsonNode tree = objectMapper.readTree(json);
            JsonNode arrayNode = tree.get(arrayNodeName);
            List<String> result = new ArrayList<>();
            if (!arrayNode.isArray()) {
                throw new IllegalStateException("Your property is not of array type. Use another method instead.");
            }
            arrayNode.forEach((node)->result.add(node.get(propertyName).asText()));
            return result;
        } catch (IOException ex) {
            return Collections.emptyList();
        }
    }

    /**
     * Parses json and finds all the particular values of sub-entities from the list of entities
     * @param json - raw json
     * @param arrayNodeName - the name of array node
     * @param subNode - the name of sub-node
     * @param propertyName - name of the property to retrieve
     * @return list of property values
     */
    public static List<String> asList(String json, String arrayNodeName, String subNode, String propertyName) {
        try {
            JsonNode tree = objectMapper.readTree(json);
            JsonNode arrayNode = tree.get(arrayNodeName);
            List<String> result = new ArrayList<>();
            if (!arrayNode.isArray()) {
                throw new IllegalStateException("Your property is not of array type. Use another method instead.");
            }
            arrayNode.forEach((node) -> result.add(node.get(subNode).get(propertyName).asText()));
            return result;
        } catch (IOException ex) {
            return Collections.emptyList();
        }
    }

}
