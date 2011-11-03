package net.ontopia.presto.spi.impl.couchdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.PrestoPagedValues;
import net.ontopia.presto.spi.utils.PrestoFieldResolver;
import net.ontopia.presto.spi.utils.PrestoPaging;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CouchTopic implements PrestoTopic {

    private static Logger log = LoggerFactory.getLogger(CouchTopic.class.getName());

    private final CouchDataProvider dataProvider;  
    private final ObjectNode data;

    protected CouchTopic(CouchDataProvider dataProvider, ObjectNode data) {
        this.dataProvider = dataProvider;
        this.data = data;    
    }

    protected CouchDataProvider getDataProvider() {
        return dataProvider;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof CouchTopic) {
            CouchTopic other = (CouchTopic)o;
            return other.getId().equals(getId());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public String toString() {
        return "CouchTopic[" + getId() + "]";
    }
    
    protected static ObjectNode newInstanceObjectNode(CouchDataProvider dataProvider, PrestoType type) {
        ObjectNode data = dataProvider.getObjectMapper().createObjectNode();
        data.put(":type", type.getId());
        return data;
    }

    ObjectNode getData() {
        return data;
    }

    @Override
    public String getId() {
        return data.get("_id").getTextValue();
    }

    @Override
    public String getName() {
        JsonNode name = data.get(":name");
        return name == null ? null : name.getTextValue();
    }

    @Override
    public String getTypeId() {
        return data.get(":type").getTextValue();
    }

    // json data access strategy

    protected ArrayNode getFieldValue(PrestoField field) {
        return getDataProvider().getFieldStrategy().getFieldValue(getData(), field);
    }

    protected void putFieldValue(PrestoField field, ArrayNode value) {
        getDataProvider().getFieldStrategy().putFieldValue(getData(), field, value);
    }
    
    // methods for retrieving the state of a couchdb document

    @Override
    public List<? extends Object> getValues(PrestoField field) {
        return getValues(field, null).getValues();            
    }

    @Override
    public PagedValues getValues(PrestoField field, int offset, int limit) {
        return getValues(field, new PrestoPaging(offset, limit));
    }

    protected PagedValues getValues(PrestoField field, Paging paging) {
        // get field values from data provider
        ObjectNode extra = (ObjectNode)field.getExtra();
        if (extra != null && extra.has("resolve")) {
            return resolveValues(field, paging, extra);
            
        } else {

            // get field values from topic data
            List<Object> values = new ArrayList<Object>();
            ArrayNode fieldNode = getFieldValue(field);

            int size = fieldNode == null ? 0 : fieldNode.size();
            int start = 0;
            int end = size;
            if (paging != null) {
                start = Math.min(Math.max(0, paging.getOffset()), size);
                end = Math.min(paging.getLimit()+start, size);
            }

            if (fieldNode != null) { 
                if (field.isReferenceField()) {
                    List<String> topicIds = new ArrayList<String>(fieldNode.size());
                    for (int i=start; i < end; i ++) {
                        JsonNode value = fieldNode.get(i);
                        if (value.isTextual()) {
                            topicIds.add(value.getTextValue());
                        }
                    }
                    values.addAll(dataProvider.getTopicsByIds(topicIds));
                } else {
                    for (int i=start; i < end; i ++) {
                        JsonNode value = fieldNode.get(i);
                        if (value.isTextual()) {
                            values.add(value.getTextValue());
                        } else {
                            values.add(value.toString());
                        }
                    }
                }
            }
            return new PrestoPagedValues(values, paging, size);
        }
    }

    private PagedValues resolveValues(PrestoField field, Paging paging, ObjectNode extra) {
        JsonNode resolveNode = extra.get("resolve");
        if (resolveNode.isArray()) {
            ArrayNode resolveArray = (ArrayNode)resolveNode;
            return resolveValues(this, field, resolveArray, paging);
        } else {
            throw new RuntimeException("extra.resolve on field " + field.getId() + " is not an array: " + resolveNode);
        }
    }

    private PagedValues resolveValues(CouchTopic topic, PrestoField field, ArrayNode resolveArray, Paging paging) {
        PagedValues result = null;
        PrestoType type = field.getSchemaProvider().getTypeById(topic.getTypeId());
        Collection<? extends Object> resultCollection = Collections.singleton(topic);
        int size = resolveArray.size();
        for (int i=0; i < size; i++) {
            boolean isLast = (i == size-1);
            boolean isReference = field.isReferenceField() || !isLast;
            ObjectNode resolveConfig = (ObjectNode)resolveArray.get(i);
            result = resolveValues(resultCollection, type, field, isReference, resolveConfig, paging);
            resultCollection = result.getValues();
        }
        return result;
    }

    private PagedValues resolveValues(Collection<? extends Object> topics,
            PrestoType type, PrestoField field, boolean isReference, ObjectNode resolveConfig, 
            Paging paging) {
        
        PrestoFieldResolver resolver = getDataProvider().createFieldResolver(type.getSchemaProvider(), resolveConfig);
        if (resolver == null) {
            return new PrestoPagedValues(Collections.emptyList(), paging, 0);            
        } else {
            if (!field.isReadOnly()) {
                log.warn("Type " + type.getId() + " field " + field.getId() + " not read-only.");
            }
            return resolver.resolve(topics, type, field, isReference, paging);
        }
    }

    // methods for updating the state of a couchdb document

    private String getValue(Object value) {
        if (value instanceof CouchTopic) {
            CouchTopic valueTopic = (CouchTopic)value;
            return valueTopic.getId();
        } else {
            return(String)value;
        }
    }

    void setValue(PrestoField field, Collection<? extends Object> values) {
        ArrayNode arrayNode = dataProvider.getObjectMapper().createArrayNode();
        for (Object value : values) {
            arrayNode.add(getValue(value));
        }
        putFieldValue(field, arrayNode);
    }

    void addValue(PrestoField field, Collection<? extends Object> values, int index) {
        if (!values.isEmpty()) {

            // remove duplicates (new)
            Set<String> addableValueIds = new HashSet<String>(values.size()); 
            for (Object value : values) {
                addableValueIds.add(getValue(value));
            }

            // remove duplicates (existing)
            Collection<String> existingValueIds = new LinkedHashSet<String>();
            ArrayNode jsonNode = getFieldValue(field);
            if (jsonNode != null) {
                for (JsonNode existing : jsonNode) {
                    if (existing.isTextual()) {
                        existingValueIds.add(existing.getTextValue());
                    }
                }
            }

            List<String> result = new ArrayList<String>(existingValueIds.size() + addableValueIds.size());
            for (String valueId : existingValueIds) {
                result.add(valueId);
            }

            // remove duplicate values and decrement calculated index
            int calculatedIndex = index >= 0 && index < existingValueIds.size() ? index : existingValueIds.size(); 
            for (String valueId : addableValueIds) {
                int valueIndex = result.indexOf(valueId);
                if (valueIndex >= 0) {
                    if (valueIndex < calculatedIndex) {
                        calculatedIndex--;
                    } 
                    result.remove(valueIndex);
                }
            }

            // insert new values at calculated index
            for (String valueId : addableValueIds) {
                result.add(calculatedIndex, valueId);
            }

            // create new array node
            ArrayNode arrayNode = dataProvider.getObjectMapper().createArrayNode();
            for (String value : result) {
                arrayNode.add(value);
            }
            putFieldValue(field, arrayNode);
        }
    }

    void removeValue(PrestoField field, Collection<? extends Object> values) {
        if (!values.isEmpty()) {
            ArrayNode jsonNode = getFieldValue(field);
            if (jsonNode != null) {
                Collection<String> existing = new LinkedHashSet<String>(jsonNode.size());
                for (JsonNode item : jsonNode) {
                    existing.add(item.getValueAsText());
                }
                for (Object value : values) {
                    existing.remove(getValue(value));
                }
                ArrayNode arrayNode  = dataProvider.getObjectMapper().createArrayNode();
                for (String value : existing) {
                    arrayNode.add(value);
                }
                putFieldValue(field, arrayNode);
            }
        }
    }

    void updateNameProperty(Collection<? extends Object> values) {
        String name;
        Object value = values.isEmpty() ? null : values.iterator().next();
        if (value == null) {
            name = "No name";
        } else if (value instanceof CouchTopic) {
            name = ((CouchTopic)value).getName();
        } else {
            name = value.toString();
        }
        getData().put(":name", name);
    }

}
