package net.ontopia.presto.spi.jackson;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoFieldUsage;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public class JacksonDefaultDataStrategy implements JacksonDataStrategy {

    private static final String ID_DEFAULT_FIELD = "_id";
    private static final String TYPE_DEFAULT_FIELD = ":type";
    private static final String NAME_DEFAULT_FIELD = ":name";

    @Override
    public String getId(ObjectNode doc) {
        return doc.get(ID_DEFAULT_FIELD).getTextValue();
    }
    
    @Override
    public String getTypeId(ObjectNode doc) {
        return doc.get(TYPE_DEFAULT_FIELD).getTextValue();
    }
    
    @Override
    public String getName(ObjectNode doc) {
        return getSingleStringFieldValue(doc, NAME_DEFAULT_FIELD);
    }
    
    protected String getSingleStringFieldValue(ObjectNode doc, String fieldId) {
        JsonNode value = doc.get(fieldId);
        return (value != null && value.isArray() && value.size() > 0 ? value.get(0).asText() : null); 
    }

    @Override
    public String getName(ObjectNode doc, PrestoFieldUsage field) {
        return getName(doc);
    }

    @Override
    public ArrayNode getFieldValue(ObjectNode doc, PrestoField field) {
        JsonNode value = doc.get(field.getActualId());
        return (ArrayNode)(value != null && value.isArray() ? value : null); 
    }

    @Override
    public void putFieldValue(ObjectNode doc, PrestoField field, ArrayNode value) {
        doc.put(field.getActualId(), value);
    }

    @Override
    public boolean hasFieldValue(ObjectNode doc, PrestoField field) {
        return doc.has(field.getActualId());
    }

}
