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
            doc = getCouchConnector().get(ObjectNode.class, topicId);
        } catch (DocumentNotFoundException e) {
            log.warn("Topic with id '" + topicId + "' not found.");
        }
        return existing(doc);
    }

    public Collection<PrestoTopic> getTopicsByIds(Collection<String> topicIds) {
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

    protected Collection<Object> getExternalValues(CouchTopic topic, PrestoField field) {
        ObjectNode extra = (ObjectNode)field.getExtra();
        String type = extra.get("type").getTextValue();
        if (type == null) {
            log.error("extra.type not specified on CouchDB field: " + field.getId());
        } else if (type.equals("query")) {
            String designDocId = extra.get("designDocId").getTextValue();
            String viewName = extra.get("viewName").getTextValue();
            
            Collection<?> keys = new ArrayList<Object>();
            if (extra.has("key")) {
                keys = replaceKeyVariables(topic, field, extra.get("key"));
                if (keys.isEmpty()) {
                    return Collections.emptyList();
                }
            } else {
                keys = Collections.singleton(topic.getId());
            }
            
            boolean includeDocs = extra.has("includeDocs") && extra.get("includeDocs").getBooleanValue();
            
            List<Object> result = new ArrayList<Object>();
            ViewQuery query = new ViewQuery()
            .designDocId(designDocId)
            .viewName(viewName)
            .keys(keys)
            .includeDocs(includeDocs);
    
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
                            if (field.isReferenceField()) {
                                PrestoTopic valueTopic = topic.getDataProvider().getTopicById(textValue);
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
            if (field.isSorted()) {
                Collections.sort(result, new Comparator<Object>() {
                    public int compare(Object o1, Object o2) {
                        String n1 = (o1 instanceof PrestoTopic) ? ((PrestoTopic)o1).getName() : (o1 == null ? null : o1.toString());
                        String n2 = (o2 instanceof PrestoTopic) ? ((PrestoTopic)o2).getName() : (o2 == null ? null : o2.toString());
                        return compareComparables(n1, n2);
                    }
                });
            }
            if (extra.has("excludeSelf") && extra.get("excludeSelf").getBooleanValue()) {
                result.remove(topic);
            }
            return result;
        } else {
            log.error("Unknown type specified on CouchDB field: " + field.getId());            
        }
        return Collections.emptyList();
    }
    
    protected Collection<JsonNode> replaceKeyVariables(CouchTopic topic, PrestoField field, JsonNode key) {
        PrestoSchemaProvider schemaProvider = field.getSchemaProvider();
        String typeId = topic.getTypeId();
        PrestoType type = schemaProvider.getTypeById(typeId);
        
        // find set of variables
        Collection<String> varNames = new HashSet<String>();
        findVariables(key, varNames);

        int totalSize = 1;
        Map<String,List<String>> varValues = new HashMap<String,List<String>>();
        for (String variable : varNames) {
            List<String> valueStrings = new ArrayList<String>();
            PrestoField valueField = type.getFieldById(variable);
            Collection<Object> values = topic.getValues(valueField);
            for (Object value : values) {
                if (value instanceof PrestoTopic) {
                    valueStrings.add((((PrestoTopic)value).getId()));
                } else {
                    valueStrings.add(value == null ? null : value.toString());
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

    protected void findVariables(JsonNode node, Collection<String> variables) {
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
    
    protected String getVariable(String value) {
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

    public PrestoChangeSet updateTopic(PrestoTopic topic) {
        return new CouchChangeSet(this, (CouchTopic)topic);
    }

    public boolean removeTopic(PrestoTopic topic, PrestoType type) {
        return removeTopic(topic, type, true);
    }

    private boolean removeTopic(PrestoTopic topic, PrestoType type, boolean removeDependencies) {
        PrestoSchemaProvider schemaProvider = type.getSchemaProvider();
        // find and remove dependencies
        if (removeDependencies) {
            for (PrestoTopic dependency : findDependencies(topic, type)) {
                if (!dependency.equals(topic)) {
                    PrestoType dependencyType = schemaProvider.getTypeById(dependency.getTypeId());
                    removeTopic(dependency, dependencyType, false);
                }
            }
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

        if (!deleted.contains(topic) && type.isRemovable()) {
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
                    if (field.isCascadingDelete() && valueType.isRemovable()) {
                        removeTopic(valueTopic, valueType);
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
