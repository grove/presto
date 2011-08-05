package net.ontopia.presto.spi.impl.couchdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoType;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.CouchDbConnector;
import org.ektorp.DocumentNotFoundException;
import org.ektorp.UpdateConflictException;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.ViewResult.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CouchDataProvider implements PrestoDataProvider {

    private Logger log = LoggerFactory.getLogger(CouchDataProvider.class.getName());

    private final ObjectMapper mapper = new ObjectMapper();

    protected CouchDbConnector db;

    protected String designDocId = "_design/presto";

    public CouchDataProvider(CouchDbConnector db) {
        this.db = db;
    }

    protected CouchDbConnector getCouchConnector() {
        return db;
    }

    protected ObjectMapper getObjectMapper() {
        return mapper;
    }

    public PrestoTopic getTopicById(String topicId) {
        // look up by document id
        ObjectNode doc = null;
        try {
            if (topicId != null) {
                doc = getCouchConnector().get(ObjectNode.class, topicId);
            }
        } catch (DocumentNotFoundException e) {
            log.warn("Topic with id '" + topicId + "' not found.");
        }
        return existing(doc);
    }

    public Collection<PrestoTopic> getTopicsByIds(Collection<String> topicIds) {
        if (topicIds.isEmpty()) {
            return Collections.emptyList();
        }
        Collection<PrestoTopic> result = new ArrayList<PrestoTopic>(topicIds.size());
        // look up by document ids
        ViewQuery query = new ViewQuery()
        .allDocs()
        .includeDocs(true).keys(topicIds);

        ViewResult viewResult = getCouchConnector().queryView(query);
        for (Row row : viewResult.getRows()) {
            JsonNode jsonNode = row.getDocAsNode();
            if (jsonNode.isObject()) {
                result.add(existing((ObjectNode)jsonNode));
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    PagedValues resolveValues(CouchTopic topic, PrestoField field, ArrayNode resolveArray, boolean paging, int offset, int limit) {
        PagedValues result = null;
        @SuppressWarnings("rawtypes")
        Collection resultCollection = Collections.singleton(topic);
        int size = resolveArray.size();
        for (int i=0; i < size; i++) {
            boolean isLast = (i == size-1);
            boolean isReference = field.isReferenceField() || (field.isPrimitiveField() && !isLast);
            result = resolveValues(resultCollection, topic.getDataProvider(), field.getSchemaProvider(), isReference, (ObjectNode)resolveArray.get(i), paging, offset, limit);
            resultCollection = result.getValues();
        }
        return result;
    }

    private PagedValues resolveValues(Collection<CouchTopic> topics, PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, 
            boolean isReference, ObjectNode resolveItem, boolean paging, int _offset, int _limit) {

        final int DEFAULT_LIMIT = 40;
        int offset = paging ?  Math.max(0, _offset): _offset;
        int limit = paging ? _limit > 0 ? _limit : DEFAULT_LIMIT : _limit;

        String type = resolveItem.get("type").getTextValue();
        if (type == null) {
            log.error("type not specified on resolve item: " + resolveItem);
        } else if (type.equals("navigate")) {

            if (resolveItem.has("path")) {
                JsonNode pathNode = resolveItem.get("path");
                if (resolveItem.isArray()) {
                    List<Object> result = new ArrayList<Object>(topics);
                    for (JsonNode pathItem : pathNode) {
                        // result = extractPathValues(result, pathItem.getTextValue());
                    }
                    return new CouchPagedValues(result, offset, limit, result.size());
                }
            }

        } else if (type.equals("query")) {
            String designDocId = resolveItem.get("designDocId").getTextValue();
            String viewName = resolveItem.get("viewName").getTextValue();

            Collection<?> keys = new ArrayList<Object>();
            if (resolveItem.has("key")) {
                keys = replaceKeyVariables(topics, schemaProvider, resolveItem.get("key"));
                if (keys.isEmpty()) {
                    return new CouchPagedValues(Collections.emptyList(), 0, _limit,0);
                }
            } else {
                Collection<String> _keys = new ArrayList<String>(topics.size());
                for (CouchTopic topic : topics) {
                    _keys.add(topic.getId());
                }
                keys = _keys;
            }

            boolean includeDocs = resolveItem.has("includeDocs") && resolveItem.get("includeDocs").getBooleanValue();

            List<Object> result = new ArrayList<Object>();
            ViewQuery query = new ViewQuery()
            .designDocId(designDocId)
            .viewName(viewName)
            .keys(keys)
            .includeDocs(includeDocs);

            if (paging) {
                if (offset > 0) {
                    query = query.skip(offset);
                }
                if (limit > 0) {
                    query = query.limit(limit);
                }
            }

            ViewResult viewResult = getCouchConnector().queryView(query);
            for (Row row : viewResult.getRows()) {
                if (includeDocs) {
                    JsonNode value = (JsonNode)row.getDocAsNode();
                    if (value != null) {
                        if (value.isObject()) {
                            result.add(existing((ObjectNode)value));
                        } else {
                            result.add(value.getTextValue());
                        }
                    }
                } else {
                    JsonNode valueAsNode = row.getValueAsNode();
                    if (valueAsNode == null) {
                        // do nothing
                    } else if (valueAsNode.isTextual()) {
                        String textValue = valueAsNode.getTextValue();
                        if (textValue != null) {
                            if (isReference) {
                                PrestoTopic valueTopic = dataProvider.getTopicById(textValue);
                                if (valueTopic != null) {
                                    result.add(valueTopic);
                                }
                            } else {
                                result.add(textValue);
                            }
                        }
                    } else {
                        result.add(valueAsNode.toString());
                    }
                }
            }
            if (resolveItem.has("excludeSelf") && resolveItem.get("excludeSelf").getBooleanValue()) {
                result.removeAll(topics);
            }
            return new CouchPagedValues(result, offset, limit, viewResult.getSize());
        } else {
            log.error("Unknown type specified on resolve item: " + resolveItem);            
        }
        return new CouchPagedValues(Collections.emptyList(), 0, limit, 0);
    }

    private Collection<JsonNode> replaceKeyVariables(Collection<CouchTopic> topics, PrestoSchemaProvider schemaProvider, JsonNode key) {
        Collection<JsonNode> result = new ArrayList<JsonNode>();
        for (CouchTopic topic : topics) {
            result.addAll(replaceKeyVariables(topic, schemaProvider, key));
        }
        return result;
    }

    private Collection<JsonNode> replaceKeyVariables(CouchTopic topic, PrestoSchemaProvider schemaProvider, JsonNode key) {
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

    public Collection<PrestoTopic> getAvailableFieldValues(PrestoFieldUsage field) {
        Collection<PrestoType> types = field.getAvailableFieldValueTypes();
        if (types.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> typeIds = new ArrayList<String>();
        for (PrestoType type : types) {
            typeIds.add(type.getId());
        }
        List<PrestoTopic> result = new ArrayList<PrestoTopic>(typeIds.size());
        ViewQuery query = new ViewQuery()
        .designDocId(designDocId)
        .viewName("by-type").includeDocs(true).keys(typeIds);

        ViewResult viewResult = getCouchConnector().queryView(query);
        for (Row row : viewResult.getRows()) {
            ObjectNode doc = (ObjectNode)row.getDocAsNode();        
            result.add(existing(doc));
        }
        Collections.sort(result, new Comparator<PrestoTopic>() {
            public int compare(PrestoTopic o1, PrestoTopic o2) {
                return compareComparables(o1.getName(), o2.getName());
            }
        });
        return result;
    }

    protected int compareComparables(String o1, String o2) {
        if (o1 == null)
            return (o2 == null ? 0 : -1);
        else if (o2 == null)
            return 1;
        else
            return o1.compareTo(o2);
    }

    public PrestoChangeSet createTopic(PrestoType type) {
        return new CouchChangeSet(this, type);
    }

    public PrestoChangeSet updateTopic(PrestoTopic topic, PrestoType type) {
        return new CouchChangeSet(this, (CouchTopic)topic, type);
    }

    public boolean deleteTopic(PrestoTopic topic, PrestoType type) {
        return deleteTopic(topic, type, true);
    }

    private boolean deleteTopic(PrestoTopic topic, PrestoType type, boolean removeDependencies) {
        // find and remove dependencies
        if (removeDependencies) {
            removeDependencies(topic, type);
        }
        // clear incoming foreign keys
        for (PrestoField field : type.getFields()) {
            if (field.getInverseFieldId() != null) {
                boolean isNew = false;
                removeInverseFieldValue(isNew, topic, field, topic.getValues(field));
            }
        }

        return delete((CouchTopic)topic);    
    }

    private void removeDependencies(PrestoTopic topic, PrestoType type) {
        PrestoSchemaProvider schemaProvider = type.getSchemaProvider();
        for (PrestoTopic dependency : findDependencies(topic, type)) {
            if (!dependency.equals(topic)) {
                PrestoType dependencyType = schemaProvider.getTypeById(dependency.getTypeId());
                deleteTopic(dependency, dependencyType, false);
            }
        }
    }

    public void close() {
    }

    abstract CouchTopic existing(ObjectNode doc);

    abstract CouchTopic newInstance(PrestoType type);

    // couchdb crud operations

    void create(CouchTopic topic) {
        getCouchConnector().create(topic.getData());
    }

    void update(CouchTopic topic) {
        getCouchConnector().update(topic.getData());
    }

    boolean delete(CouchTopic topic) {
        log.info("Removing: " + topic.getId() + " " + topic.getName());
        try {
            getCouchConnector().delete(topic.getData());
            return true;
        } catch (UpdateConflictException e) {
            CouchTopic topic2 = (CouchTopic)getTopicById(topic.getId());
            if (topic2 != null) {
                getCouchConnector().delete(topic2.getData());
                return true;
            } else {
                return false;
            }
        }
    }

    // dependent topics / cascading deletes

    private Collection<PrestoTopic> findDependencies(PrestoTopic topic, PrestoType type) {
        Collection<PrestoTopic> dependencies = new HashSet<PrestoTopic>();
        findDependencies(topic, type, dependencies);
        return dependencies;
    }

    private void findDependencies(PrestoTopic topic, PrestoType type, Collection<PrestoTopic> deleted) {

        if (!deleted.contains(topic) && type.isRemovableCascadingDelete()) {
            for (PrestoField field : type.getFields()) {
                if (field.isReferenceField()) {
                    if (field.isCascadingDelete()) { 
                        PrestoSchemaProvider schemaProvider = type.getSchemaProvider();
                        for (Object value : topic.getValues(field)) {
                            PrestoTopic valueTopic = (PrestoTopic)value;
                            String typeId = valueTopic.getTypeId();
                            PrestoType valueType = schemaProvider.getTypeById(typeId);
                            deleted.add(valueTopic);
                            findDependencies(valueTopic, valueType, deleted);
                        }
                    }
                }
            }
        }
    }

    // inverse fields (foreign keys)

    void addInverseFieldValue(boolean isNew, PrestoTopic topic, PrestoField field, Collection<?> values) {
        String inverseFieldId = field.getInverseFieldId();
        if (inverseFieldId != null) {
            for (Object value : values) {

                CouchTopic valueTopic = (CouchTopic)value;
                PrestoType type = field.getSchemaProvider().getTypeById(valueTopic.getTypeId());
                PrestoField inverseField = type.getFieldById(inverseFieldId);

                int index = -1;
                valueTopic.addValue(inverseField, Collections.singleton(topic), index);
                update(valueTopic);      
            }
        }
    }

    void removeInverseFieldValue(boolean isNew, PrestoTopic topic, PrestoField field, Collection<?> values) {
        if (!isNew) {
            String inverseFieldId = field.getInverseFieldId();
            if (inverseFieldId != null) {
                for (Object value : values) {

                    CouchTopic valueTopic = (CouchTopic)value;
                    PrestoType valueType = field.getSchemaProvider().getTypeById(valueTopic.getTypeId());
                    if (field.isCascadingDelete() && valueType.isRemovableCascadingDelete()) {
                        deleteTopic(valueTopic, valueType);
                    } else {          
                        PrestoField inverseField = valueType.getFieldById(inverseFieldId);
                        valueTopic.removeValue(inverseField, Collections.singleton(topic));
                        update(valueTopic);
                    }
                }
            }
        }
    }

}
