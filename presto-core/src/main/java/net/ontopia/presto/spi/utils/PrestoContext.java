package net.ontopia.presto.spi.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

public class PrestoContext {

    private final PrestoDataProvider dataProvider;
    private final PrestoSchemaProvider schemaProvider;
    private final ObjectMapper mapper;

    public PrestoContext(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, ObjectMapper mapper) {
        this.dataProvider = dataProvider;
        this.schemaProvider = schemaProvider;
        this.mapper = mapper;
    }
    
    public Collection<JsonNode> replaceVariables(Collection<? extends Object> topics, JsonNode key) {
        Collection<JsonNode> result = new ArrayList<JsonNode>();
        for (Object topic : topics) {
            if (topic instanceof PrestoTopic) {
                result.addAll(replaceVariables((PrestoTopic)topic, key));
            }
        }
        return result;
    }

    private Collection<JsonNode> replaceVariables(PrestoTopic topic, JsonNode key) {
        String typeId = topic.getTypeId();
        PrestoType type = getSchemaProvider().getTypeById(typeId);

        // find set of variables
        Collection<String> varNames = new HashSet<String>();
        findVariables(key, varNames);

        int totalSize = 1;
        Map<String,List<String>> varValues = new HashMap<String,List<String>>();
        for (String variable : varNames) {
            List<String> valueStrings = new ArrayList<String>();
            if (variable.equals(":id")) {
                valueStrings.add(topic.getId());                
            } else if (variable.equals(":type")) {
                valueStrings.add(topic.getTypeId());                
            } else {
                PrestoField valueField = type.getFieldById(variable);
                Collection<Object> values = topic.getValues(valueField);
                for (Object value : values) {
                    if (value instanceof PrestoTopic) {
                        valueStrings.add((((PrestoTopic)value).getId()));
                    } else {
                        valueStrings.add(value == null ? null : value.toString());
                    }
                }
            }
            varValues.put(variable, valueStrings);
            totalSize = totalSize * valueStrings.size();
        }
        int varCount = varValues.size(); // number of variables
        int arraySize = totalSize/varCount; // size of array
        int arrayCount = varCount * arraySize;
        // A:[1,2] B[5,6,7] C[0] -> [1,5,0] [1,6,0] [1,7,0] 
        //                          [2,5,0] [2,6,0] [2,7,0]

        // make keys from cross-product of variable values
        Map<String,String> map = new HashMap<String,String>();
        Collection<JsonNode> keys = new ArrayList<JsonNode>();
        for (int aindex=0; aindex < arrayCount; aindex++) {
            for (String vn : varNames) {
                List<String> values = varValues.get(vn);
                int vindex = aindex % values.size();
                map.put(vn, values.get(vindex));
            }
            keys.add(replaceVariables(map, key));
        }
        return keys;
    }

    private JsonNode replaceVariables(Map<String, String> variables, JsonNode node) {
        JsonNodeFactory nodeFactory = getObjectMapper().getNodeFactory();
        if (node.isObject()) {
            ObjectNode onode = (ObjectNode)node;
            List<String> keys = new ArrayList<String>(onode.size());
            Iterator<String> kiter = onode.getFieldNames();
            while (kiter.hasNext()) {
                keys.add(kiter.next());
            }
            ObjectNode result = nodeFactory.objectNode();
            for (String key : keys) {
                String variable = getVariable(key);
                if (variable != null) {
                    result.put(variables.get(variable), replaceVariables(variables, onode.get(key)));
                } else {
                    result.put(key, replaceVariables(variables, onode.get(key)));
                }
            }
            return result;
        } else if (node.isArray()) {
            ArrayNode anode = (ArrayNode)node;
            ArrayNode result  = nodeFactory.arrayNode();
            int size = anode.size();
            for (int i=0; i < size; i++) {
                result.add(replaceVariables(variables, anode.get(i)));
            }
            return result;
        } else if (node.isTextual()) {
            String variable = getVariable(node.getTextValue());
            if (variable != null) {
                return nodeFactory.textNode(variables.get(variable));
            } else {
                return nodeFactory.textNode(node.getTextValue());
            }
        }
        return getObjectMapper().valueToTree(node);
    }

    private void findVariables(JsonNode node, Collection<String> variables) {
        if (node.isTextual()) {
            String variable = getVariable(node.getTextValue());
            if (variable != null) {
                variables.add(variable);
            }            
        } else if (node.isObject()) {
            ObjectNode onode = (ObjectNode)node;
            Iterator<Entry<String, JsonNode>> fields = onode.getFields();
            while (fields.hasNext()) {
                Entry<String,JsonNode> field = fields.next();
                String variable = getVariable(field.getKey());
                if (variable != null) {
                    variables.add(variable);
                }
                findVariables(field.getValue(), variables);
            }
        } else {
            for (JsonNode child : node) {
                findVariables(child, variables);
            }
        }
    }

    private String getVariable(String value) {
        if (value.startsWith("$")) {
            return value.substring(1);
        }
        return null;
    }

    public PrestoDataProvider getDataProvider() {
        return dataProvider;
    }

    public PrestoSchemaProvider getSchemaProvider() {
        return schemaProvider;
    }

    public ObjectMapper getObjectMapper() {
        return mapper;
    }

}
