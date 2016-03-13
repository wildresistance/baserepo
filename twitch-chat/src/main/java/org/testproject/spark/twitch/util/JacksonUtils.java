package org.testproject.spark.twitch.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.NullNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by akurilyonok on 6/13/2014.
 */
public class JacksonUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static List<String> asList(String json, String nodeName, String keyName) {
        try {
            JsonNode tree = objectMapper.readTree(json);
            JsonNode arrayNode = tree.get(nodeName);
            List<String> result = new ArrayList<String>();
            if (!arrayNode.isArray()) {
                throw new IllegalStateException("Your property is not of array type. Use another method instead.");
            }
            for (JsonNode node: arrayNode) {
                result.add(node.get(keyName).asText());
            }
            return result;
        } catch (IOException ex) {
            return Collections.emptyList();
        }
    }

    public static List<String> asList(String json, String nodeName, String keyName, String innerKeyName) {
        try {
            JsonNode tree = objectMapper.readTree(json);
            JsonNode arrayNode = tree.get(nodeName);
            List<String> result = new ArrayList<String>();
            if (!arrayNode.isArray()) {
                throw new IllegalStateException("Your property is not of array type. Use another method instead.");
            }
            for (JsonNode node: arrayNode) {
                result.add(node.get(keyName).get(innerKeyName).asText());
            }
            return result;
        } catch (IOException ex) {
            return Collections.emptyList();
        }
    }



    public JsonNode asChildNode(String json, String keyName) {
        try {
            JsonNode tree = objectMapper.readTree(json);
            return tree.findValue(keyName);
        } catch (IOException ex) {
            return NullNode.getInstance();
        }
    }
}
