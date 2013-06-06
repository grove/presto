package net.ontopia.presto.spi.jackson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.DefaultDataProvider;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.DefaultTopic;
import net.ontopia.presto.spi.utils.PrestoPagedValues;
import net.ontopia.presto.spi.utils.PrestoPaging;
import net.ontopia.presto.spi.utils.PrestoTopicFieldVariableResolver;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JacksonTopic implements DefaultTopic {

    private static Logger log = LoggerFactory.getLogger(JacksonTopic.class);

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
        return "Topic[" + getId() + "]";
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
    public String getName(PrestoFieldUsage field) {
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
    
    // methods for retrieving the state of a topic

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
            JsonNode resolveConfig = extra.get("resolve");
            if (!field.isReadOnly()) {
                log.warn("Field {} not read-only. Resolve config: {}", field.getId(), resolveConfig);
            }

            PrestoVariableResolver variableResolver = new PrestoTopicFieldVariableResolver(field.getSchemaProvider());
            return dataProvider.resolveValues(Collections.singleton(this), field, paging, resolveConfig, variableResolver);
        }
        return getValuesFromField(field, paging);
    }

    private PagedValues getValuesFromField(PrestoField field, Paging paging) {
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
                if (field.isInline()) {
                    for (int i=start; i < end; i ++) {
                        JsonNode value = fieldNode.get(i);
                        if (value.isObject()) {
                            values.add(new JacksonInlineTopic(dataProvider, (ObjectNode)value));
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
        return new PrestoPagedValues(values, paging, size);
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

            // remove duplicates (new)
            Set<Object> addableValues = new LinkedHashSet<Object>(values.size()); 
            for (Object value : values) {
                addableValues.add(convertExternalToNeutral(value));
            }

            // remove duplicates (existing)
            Set<Object> existingValues = new LinkedHashSet<Object>();
            ArrayNode jsonNode = getFieldValue(field);
            if (jsonNode != null) {
                for (JsonNode existing : jsonNode) {
                    existingValues.add(convertInternalToNeutralValue(existing));
                }
            }

            List<Object> result = new ArrayList<Object>(existingValues.size() + addableValues.size());
            for (Object value : existingValues) {
                result.add(value);
            }

            // remove duplicate values and decrement calculated index
            int calculatedIndex = index >= 0 && index < existingValues.size() ? index : existingValues.size();
            if (!result.isEmpty()) {
                for (Object value : addableValues) {
                    int valueIndex = result.indexOf(value);
                    if (valueIndex >= 0) {
                        if (valueIndex < calculatedIndex) {
                            calculatedIndex--;
                        } 
                        result.remove(valueIndex);
                    }
                }
            }
            
            // insert new values at calculated index
            if (calculatedIndex > 0) {
                for (Object value : addableValues) {
                    result.add(calculatedIndex, value);
                }
            } else {
                for (Object value : addableValues) {
                    result.add(value);
                }
            }
            // create new array node
            ArrayNode arrayNode = dataProvider.getObjectMapper().createArrayNode();
            for (Object ev : result) {
                addNeutralValue(arrayNode, ev);
            }
            putFieldValue(field, arrayNode);
        }
    }

    @Override
    public void removeValue(PrestoField field, Collection<? extends Object> values) {
        if (!values.isEmpty()) {
            ArrayNode jsonNode = getFieldValue(field);
            if (jsonNode != null) {
                Collection<Object> existing = new LinkedHashSet<Object>(jsonNode.size());
                for (JsonNode item : jsonNode) {
                    existing.add(convertInternalToNeutralValue(item));
                }
                for (Object value : values) {
                    // TODO: internal topics will not be exactly the same, but share id
                    existing.remove(convertExternalToNeutral(value));
                }
                ArrayNode arrayNode  = dataProvider.getObjectMapper().createArrayNode();
                for (Object ev : existing) {
                    addNeutralValue(arrayNode, ev);
                }
                putFieldValue(field, arrayNode);
            }
        }
    }

}
