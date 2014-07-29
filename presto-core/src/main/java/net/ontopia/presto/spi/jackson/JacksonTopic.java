package net.ontopia.presto.spi.jackson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.DefaultDataProvider;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.DefaultTopic;
import net.ontopia.presto.spi.utils.PrestoPagedValues;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public class JacksonTopic implements DefaultTopic {

    protected final JacksonDataProvider dataProvider;  
    protected final ObjectNode data;

    public JacksonTopic(JacksonDataProvider dataProvider, ObjectNode data) {
        this.dataProvider = dataProvider;
        this.data = data;    
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof JacksonTopic) {
            JacksonTopic other = (JacksonTopic)o;
            if (other.isInline()) {
                return false;
            } else {
                return other.getId().equals(getId());
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public String toString() {
        return "Topic[" + getId() + " " + getName() + "]";
    }

    public ObjectNode getData() {
        return data;
    }

    @Override
    public boolean isInline() {
        return false;
    }

    protected JacksonDataStrategy getDataStrategy() {
        return dataProvider.getDataStrategy();
    }

    @Override
    public String getId() {
        return getDataStrategy().getId(getData());
    }

    @Override
    public String getName() {
        return getDataStrategy().getName(getData());
    }

    @Override
    public String getName(PrestoField field) {
        return getDataStrategy().getName(getData(), field);
    }

    @Override
    public String getTypeId() {
        return getDataStrategy().getTypeId(getData());
    }

    // json data access strategy

    protected ArrayNode getFieldValue(PrestoField field) {
        return getDataStrategy().getFieldValue(getData(), field);
    }

    protected void putFieldValue(PrestoField field, ArrayNode value) {
        getDataStrategy().putFieldValue(getData(), field, value);
    }
    
    protected void clearFieldValue(PrestoField field) {
        getDataStrategy().clearFieldValue(getData(), field);
    }
    
    // methods for retrieving the state of a topic

    @Override
    public boolean hasValue(PrestoField field) {
        return getDataStrategy().hasFieldValue(getData(), field);
    }
    
    @Override
    public List<? extends Object> getValues(PrestoField field) {
        return dataProvider.resolveValues(this, field);
    }

    @Override
    public PagedValues getValues(PrestoField field, Projection projection) {
        return dataProvider.resolveValues(this, field, projection);
    }
    
    @Override
    public List<? extends Object> getStoredValues(PrestoField field) {
        return getValuesFromField(field, null).getValues();            
    }

    @Override
    public PagedValues getStoredValues(PrestoField field, Projection projection) {
        return getValuesFromField(field, projection);
    }

    private PagedValues getValuesFromField(PrestoField field, Projection projection) {
        // get field values from topic data
        List<Object> values = new ArrayList<Object>();
        ArrayNode fieldNode = getFieldValue(field);

        int size = fieldNode == null ? 0 : fieldNode.size();
        int start = 0;
        int end = size;
        if (projection != null && projection.isPaged()) {
            start = Math.min(Math.max(0, projection.getOffset()), size);
            end = Math.min(projection.getLimit()+start, size);
        }

        if (fieldNode != null) { 
            if (field.isReferenceField()) {
                if (field.isInline()) {
                    boolean inlineReference = field.getInlineReference() != null;
                    for (int i=start; i < end; i ++) {
                        JsonNode value = fieldNode.get(i);
                        if (inlineReference) {
                            if (value.isTextual()) {
                                values.add(value.getTextValue());
                            }
                        } else {
                            if (value.isObject()) {
                                values.add(new JacksonInlineTopic(dataProvider, (ObjectNode)value));
                            }
                        }
                    }
                } else {
                    List<String> topicIds = new ArrayList<String>(fieldNode.size());
                    for (int i=start; i < end; i ++) {
                        JsonNode value = fieldNode.get(i);
                        if (value.isTextual()) {
                            topicIds.add(value.getTextValue());
                        }
                    }
                    values.addAll(dataProvider.getTopicsByIds(topicIds));
                }
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
        return new PrestoPagedValues(values, projection, size);
    }

    // --- DefaultTopic implementation
    
    @Override
    public DefaultDataProvider getDataProvider() {
        return dataProvider;
    }
    
    @Override
    public void updateNameProperty(Collection<? extends Object> values) {
        String name;
        Object value = values.isEmpty() ? null : values.iterator().next();
        if (value == null) {
            name = "No name";
        } else if (value instanceof PrestoTopic) {
            name = ((PrestoTopic)value).getName();
        } else {
            name = value.toString();
        }
        getData().put(":name", name);
    }
    
    private Object convertInternalToNeutralValue(JsonNode value) {
        if (value.isObject()) {
            return value;
        } else if (value.isTextual()) {
            return value.asText();
        } else {
            throw new RuntimeException();
        }
    }
    
    private Object convertExternalToNeutral(Object value) {
        if (value instanceof PrestoTopic) {
            PrestoTopic valueTopic = (PrestoTopic)value;
            if (valueTopic.isInline()) {
                JacksonTopic jt = (JacksonTopic)valueTopic;
                return jt.getData();
            } else {
                return valueTopic.getId();
            }
        } else {
            return (String)value;
        }
    }

    private String getNeutralValueKey(PrestoField field, Object value) {
        if (field.isInline()) {
            ObjectNode node = (ObjectNode)value;
            String id = node.path("_id").getTextValue();
            if (id != null) {
                return id;
            } else {
                throw new RuntimeException("Cannot find object id in inline object: " + node);
            }
        } else {
            return (String)value;
        }
    }

    @Override
    public void setValue(PrestoField field, Collection<? extends Object> values) {
        ArrayNode arrayNode = dataProvider.getObjectMapper().createArrayNode();
        for (Object value : values) {
            Object ev = convertExternalToNeutral(value);
            addNeutralValue(arrayNode, ev);
        }
        putFieldValue(field, arrayNode);
    }

    private void addNeutralValue(ArrayNode arrayNode, Object ev) {
        if (ev instanceof ObjectNode) {
            arrayNode.add((ObjectNode)ev);
        } else {
            arrayNode.add((String)ev);
        }
    }

    @Override
    public void addValue(PrestoField field, Collection<? extends Object> values, int index) {
        if (!values.isEmpty()) {
            ArrayNode arrayNode = dataProvider.getObjectMapper().createArrayNode();

            ArrayNode existingNode = getFieldValue(field);

            if (existingNode == null || existingNode.size() == 0) {
                Set<String> addableValues = new LinkedHashSet<String>(values.size()); 
                for (Object addableValue : values) {
                    Object addableNeutralValue = convertExternalToNeutral(addableValue);
                    String addableNeutralValueKey = getNeutralValueKey(field, addableNeutralValue);
                    if (!addableValues.contains(addableNeutralValueKey)) {
                        addNeutralValue(arrayNode, addableNeutralValue);
                        addableValues.add(addableNeutralValueKey);
                    }
                }
            } else {
                
                // new values
                Map<String,Object> addableValues = new LinkedHashMap<String,Object>(values.size()); 
                for (Object addableValue : values) {
                    Object addableNeutralValue = convertExternalToNeutral(addableValue);
                    String addableNeutralValueKey = getNeutralValueKey(field, addableNeutralValue);
                    addableValues.put(addableNeutralValueKey, addableNeutralValue);
                }

                int existingSize = existingNode.size();
                
                Map<String,Object> result = new LinkedHashMap<String,Object>(existingSize + addableValues.size());
                
                int c=0;
                for (JsonNode existing : existingNode) {
                    if (c == index) {
                        addAddableValuesToMap(addableValues, result);
                    }
                    c++;

                    Object existingNeutralValue = convertInternalToNeutralValue(existing);
                    String existingNeutralValueKey = getNeutralValueKey(field, existingNeutralValue);

                    if (addableValues.containsKey(existingNeutralValueKey) || result.containsKey(existingNeutralValueKey)) {
                        continue;
                    } else {
                        result.put(existingNeutralValueKey, existingNeutralValue);
                    }
                }
                if (index < 0 || index >= c) {
                    addAddableValuesToMap(addableValues, result);
                }
                for (Object ev : result.values()) {
                    addNeutralValue(arrayNode, ev);
                }
            }
            putFieldValue(field, arrayNode);
        }
    }

    private void addAddableValuesToMap(Map<String, Object> addableValues, Map<String, Object> result) {
        for (String addableNeutralValueKey : addableValues.keySet()) {
            if (result.containsKey(addableNeutralValueKey)) {
                continue;
            }
            Object addableNeutralValue = addableValues.get(addableNeutralValueKey);
            result.put(addableNeutralValueKey, addableNeutralValue);
        }
    }

    @Override
    public void removeValue(PrestoField field, Collection<? extends Object> values) {
        if (!values.isEmpty()) {
            ArrayNode jsonNode = getFieldValue(field);
            if (jsonNode != null) {
                ArrayNode arrayNode  = dataProvider.getObjectMapper().createArrayNode();
                Map<String, Object> existing = new LinkedHashMap<String,Object>(jsonNode.size());
                for (JsonNode item : jsonNode) {
                    Object existingValue = convertInternalToNeutralValue(item);
                    String key = getNeutralValueKey(field, existingValue);
                    if (!existing.containsKey(key)) {
                        existing.put(key, existingValue);
                    }
                }
                for (Object value : values) {
                    Object removableValue = convertExternalToNeutral(value);
                    String id = getNeutralValueKey(field, removableValue);
                    existing.remove(id);
                }
                for (Object ev : existing.values()) {
                    addNeutralValue(arrayNode, ev);
                }
                putFieldValue(field, arrayNode);
            }
        }
    }
    
    @Override
    public void clearValue(PrestoField field) {
        clearFieldValue(field);
    }
    
    @Override
    public Object getInternalData() {
        return data;
    }
    
}
