package net.ontopia.presto.spi.impl.couchdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoType;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.ViewResult.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CouchQueryResolver {

    private Logger log = LoggerFactory.getLogger(CouchQueryResolver.class.getName());

    private final PrestoSchemaProvider schemaProvider;
    private final CouchDataProvider dataProvider;

    CouchQueryResolver(CouchDataProvider dataProvider, PrestoSchemaProvider schemaProvider) {
        this.dataProvider = dataProvider;
        this.schemaProvider = schemaProvider;
    }

    @SuppressWarnings("unchecked")
    public PagedValues resolveValues(CouchTopic topic, PrestoField field, ArrayNode resolveArray, boolean paging, int offset, int limit) {
        PagedValues result = null;
        @SuppressWarnings("rawtypes")
        Collection resultCollection = Collections.singleton(topic);
        int size = resolveArray.size();
        for (int i=0; i < size; i++) {
            boolean isLast = (i == size-1);
            boolean isReference = field.isReferenceField() || !isLast;
            result = resolveValues(resultCollection, isReference, (ObjectNode)resolveArray.get(i), paging, offset, limit);
            resultCollection = result.getValues();
        }
        return result;
    }

    private PagedValues resolveValues(Collection<CouchTopic> topics,
            boolean isReference, ObjectNode resolveItem, boolean paging, int _offset, int _limit) {

        int offset = paging ?  Math.max(0, _offset): _offset;
        int limit = paging ? _limit > 0 ? _limit : CouchTopic.DEFAULT_LIMIT : _limit;

        String type = resolveItem.get("type").getTextValue();
        if (type == null) {
            log.error("type not specified on resolve item: " + resolveItem);

        } else if (type.equals("traverse")) {
            return resolvePath(topics, resolveItem, offset, limit);

        } else if (type.equals("query")) {
            return resolveQuery(topics, isReference, resolveItem, paging, _limit, offset, limit);

        } else {
            log.error("Unknown type specified on resolve item: " + resolveItem);            
        }
        return new CouchPagedValues(Collections.emptyList(), 0, limit, 0);
    }

    private PagedValues resolveQuery(Collection<CouchTopic> topics,
            boolean isReference, ObjectNode resolveItem, 
            boolean paging, int _limit, int offset, int limit) {

        String designDocId = resolveItem.get("designDocId").getTextValue();
        String viewName = resolveItem.get("viewName").getTextValue();

        boolean includeDocs = resolveItem.has("includeDocs") && resolveItem.get("includeDocs").getBooleanValue();

        ViewQuery query = new ViewQuery()
        .designDocId(designDocId)
        .viewName(viewName)
        .reduce(false)
        .includeDocs(includeDocs);

        Collection<?> keys = new ArrayList<Object>();
        Object startKey = null;
        Object endKey = null;

        if (resolveItem.has("key")) {
            keys = replaceKeyVariables(topics, resolveItem.get("key"));
            if (keys.isEmpty()) {
                return new CouchPagedValues(Collections.emptyList(), 0, _limit,0);
            }
            query = query.keys(keys);

        } else if (resolveItem.has("startKey") && resolveItem.has("endKey")) {

            Collection<?> startKeys = replaceKeyVariables(topics, resolveItem.get("startKey"));            
            Collection<?> endKeys = replaceKeyVariables(topics, resolveItem.get("endKey"));
            
            if (startKeys.size() != endKeys.size()) {
                throw new RuntimeException("startKey and endKey of different sizes: " + startKeys + " and " + endKeys);
            }
            
            if (startKeys.isEmpty()) {
                return new CouchPagedValues(Collections.emptyList(), 0, _limit,0);
            }
            
            if (startKeys.size() > 1) {
                throw new RuntimeException("startKey or endKey not a single value: " + startKeys + " and " + endKeys);
            }

            startKey = startKeys.iterator().next();
            query = query.startKey(startKey);
            
            endKey = endKeys.iterator().next();
            query = query.endKey(endKey);

        } else {
            Collection<String> _keys = new ArrayList<String>(topics.size());
            for (CouchTopic topic : topics) {
                _keys.add(topic.getId());
            }
            keys = _keys;
            query = query.keys(keys);
        }

        if (paging) {
            if (offset > 0) {
                query = query.skip(offset);
            }
            if (limit > 0) {
                query = query.limit(limit);
            }
        }

        List<Object> result = new ArrayList<Object>();        
        ViewResult viewResult = dataProvider.getCouchConnector().queryView(query);

        if (includeDocs) {
            for (Row row : viewResult.getRows()) {
                JsonNode value = (JsonNode)row.getDocAsNode();
                if (value != null) {
                    if (value.isObject()) {
                        result.add(dataProvider.existing((ObjectNode)value));
                    } else {
                        result.add(value.getTextValue());
                    }
                }
            }
        } else {
            List<String> values = new ArrayList<String>();        
            for (Row row : viewResult.getRows()) {
                JsonNode valueAsNode = row.getValueAsNode();
                if (valueAsNode == null) {
                    // do nothing
                } else if (valueAsNode.isTextual()) {
                    String textValue = valueAsNode.getTextValue();
                    if (textValue != null) {
                        result.add(textValue);
                    }
                } else {
                    result.add(valueAsNode.toString());
                }
            }
            if (isReference) {
                result.addAll(dataProvider.getTopicsByIds(values));
            } else {
                result.addAll(values);
            }
        }
        if (resolveItem.has("excludeSelf") && resolveItem.get("excludeSelf").getBooleanValue()) {
            result.removeAll(topics);
        }
        int totalSize = viewResult.getSize();
        if (paging && !(totalSize < limit)) {
            if (resolveItem.has("count") && resolveItem.get("count").getTextValue().equals("reduce-value")) {
                ViewQuery countQuery = new ViewQuery()
                .designDocId(designDocId)
                .viewName(viewName)                
                .startKey(startKey)
                .endKey(endKey)
                .reduce(true);
                ViewResult countViewResult = dataProvider.getCouchConnector().queryView(countQuery);
                for (Row row : countViewResult.getRows()) {
                    totalSize = row.getValueAsInt();
                }
            }
        }
        return new CouchPagedValues(result, offset, limit, totalSize);
    }

    private PagedValues resolvePath(Collection<CouchTopic> topics,
            ObjectNode resolveItem, int offset, int limit) {
        if (resolveItem.has("path")) {
            JsonNode pathNode = resolveItem.get("path");
            if (pathNode.isArray()) {
                Collection<Object> objects = new HashSet<Object>(topics);
                for (JsonNode fieldItem : pathNode) {
                    // TODO: allow optional recursion
                    String fieldId = fieldItem.getTextValue();
                    objects = traverseField(objects, fieldId);
                }
                List<Object> result = new ArrayList<Object>(objects);
                return new CouchPagedValues(result, offset, limit, result.size());
            }
        }
        return new CouchPagedValues(Collections.emptyList(), 0, limit, 0);        
    }

    private Collection<Object> traverseField(Collection<Object> objects, String fieldId) {
        Collection<Object> result = new HashSet<Object>();
        for (Object object : objects) {
            if (object instanceof PrestoTopic) {
                PrestoTopic topic = (PrestoTopic)object;
                String typeId = topic.getTypeId();
                PrestoType type = schemaProvider.getTypeById(typeId);
                try {
                    PrestoField field = type.getFieldById(fieldId);
                    List<Object> values = topic.getValues(field);
                    result.addAll(values);
                } catch (Exception e) {
                    log.warn("Object " + topic.getId() + " does not have field '" + fieldId + "'");
                }
            } else {
                log.warn("Value " + object + " does not have field '" + fieldId + "'");
            }
        }
        return result;
    }

    private Collection<JsonNode> replaceKeyVariables(Collection<CouchTopic> topics, JsonNode key) {
        Collection<JsonNode> result = new ArrayList<JsonNode>();
        for (CouchTopic topic : topics) {
            result.addAll(replaceKeyVariables(topic, key));
        }
        return result;
    }

    private Collection<JsonNode> replaceKeyVariables(CouchTopic topic, JsonNode key) {
        String typeId = topic.getTypeId();
        PrestoType type = schemaProvider.getTypeById(typeId);

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
        ObjectMapper mapper = dataProvider.getObjectMapper();
        JsonNodeFactory nodeFactory = mapper.getNodeFactory();
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
        return mapper.valueToTree(node);
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

}
