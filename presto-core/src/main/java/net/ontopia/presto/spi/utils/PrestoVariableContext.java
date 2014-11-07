package net.ontopia.presto.spi.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoSchemaProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class PrestoVariableContext {

    private final PrestoSchemaProvider schemaProvider;
    private final PrestoDataProvider dataProvider;
    private final ObjectMapper mapper;

    public PrestoVariableContext(PrestoSchemaProvider schemaProvider, PrestoDataProvider dataProvider, ObjectMapper mapper) {
        this.schemaProvider = schemaProvider;
        this.dataProvider = dataProvider;
        this.mapper = mapper;
    }
    
    public Collection<JsonNode> replaceVariables(PrestoVariableResolver variableResolver, Collection<? extends Object> values, JsonNode key) {
        if (key.isMissingNode()) {
            return Collections.emptyList();
        }
        if (values.isEmpty()) {
            return replaceVariablesValue(variableResolver, null, key);
               
        } else {
            Collection<JsonNode> result = new ArrayList<JsonNode>();
            for (Object value : values) {
                result.addAll(replaceVariablesValue(variableResolver, value, key));
            }
            return result;
        }
    }

    private Collection<JsonNode> replaceVariablesValue(PrestoVariableResolver variableResolver, Object value, JsonNode key) {
        // find set of variables
        Collection<String> varNames = new HashSet<String>();
        findVariables(key, varNames);
        if (varNames.isEmpty()) {
            return Collections.singletonList(key);
        }

        int totalSize = 1;
        Map<String,List<? extends Object>> varValues = new HashMap<String,List<? extends Object>>();
        for (String variable : varNames) {
            List<? extends Object> valueStrings = variableResolver.getValues(value, variable);
            varValues.put(variable, valueStrings);
            totalSize = totalSize * valueStrings.size();
        }
        int varCount = varValues.size(); // number of variables
        int arraySize; // size of array
        if (totalSize == 0) {
            arraySize = 0;
        } else if (totalSize == 1) {
            arraySize = 1;
        } else {
            arraySize = totalSize/varCount;
        }
        int arrayCount = varCount * arraySize;
        // A:[1,2] B[5,6,7] C[0] -> [1,5,0] [1,6,0] [1,7,0] 
        //                          [2,5,0] [2,6,0] [2,7,0]
        // A:[1] B[2,3] -> [1, 2], [1, 3]
        // A:[1] B[2] -> [1, 2]
        // A:[] B[2] -> null
        
        // make keys from cross-product of variable values
        Map<String,Object> map = new HashMap<String,Object>();
        Collection<JsonNode> keys = new ArrayList<JsonNode>();
        for (int aindex=0; aindex < arrayCount; aindex++) {
            for (String vn : varNames) {
                List<? extends Object> values = varValues.get(vn);
                int vindex = aindex % values.size();
                map.put(vn, values.get(vindex));
            }
            keys.add(replaceVariables(map, key));
        }
        return keys;
    }

    private JsonNode replaceVariables(Map<String, ? extends Object> variables, JsonNode node) {
        JsonNodeFactory nodeFactory = getObjectMapper().getNodeFactory();
        if (node.isObject()) {
            ObjectNode onode = (ObjectNode)node;
            List<String> keys = new ArrayList<String>(onode.size());
            Iterator<String> kiter = onode.fieldNames();
            while (kiter.hasNext()) {
                keys.add(kiter.next());
            }
            ObjectNode result = nodeFactory.objectNode();
            for (String key : keys) {
                if (isVariable(key)) {
                    Object text = variables.get(getVariable(key));
                    result.put(toValueString(text), replaceVariables(variables, onode.get(key)));
                } else {
                    result.put(getKey(key), replaceVariables(variables, onode.get(key)));
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
            String key = node.textValue();
            if (isVariable(key)) {
                Object text = variables.get(getVariable(key));
                return nodeFactory.textNode(toValueString(text));
            } else {
                return nodeFactory.textNode(getKey(key));
            }
        }
        return getObjectMapper().valueToTree(node);
    }

    private String toValueString(Object value) {
        return value == null ? null : value.toString();
    }
    
    private void findVariables(JsonNode node, Collection<String> variables) {
        if (node.isTextual()) {
            String key = node.textValue();
            if (isVariable(key)) {
                variables.add(getVariable(key));
            }            
        } else if (node.isObject()) {
            ObjectNode onode = (ObjectNode)node;
            Iterator<Entry<String, JsonNode>> fields = onode.fields();
            while (fields.hasNext()) {
                Entry<String,JsonNode> field = fields.next();
                String key = field.getKey();
                if (isVariable(key)) {
                    variables.add(getVariable(key));
                }
                findVariables(field.getValue(), variables);
            }
        } else {
            for (JsonNode child : node) {
                findVariables(child, variables);
            }
        }
    }

    private boolean isVariable(String value) {
        if (value.length() > 1 && value.charAt(0) == '$' && 
                (Character.isLetter(value.charAt(1)) || value.charAt(1) == ':')) {
            return true;
        }
        return false;
    }
    
    private String getVariable(String value) {
        if (isVariable(value)) {
            // proper variable
            return value.substring(1);
        }
        throw new RuntimeException("Illegal variable: " + value);
    }
    
    private String getKey(String value) {
        if (isVariable(value)) {
            throw new RuntimeException("Illegal key: " + value);
        }
        return value;
    }

    public PrestoSchemaProvider getSchemaProvider() {
        return schemaProvider;
    }

    public PrestoDataProvider getDataProvider() {
        return dataProvider;
    }

    public ObjectMapper getObjectMapper() {
        return mapper;
    }

}
