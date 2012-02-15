package net.ontopia.presto.spi.jackson;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoFieldUsage;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public class JacksonDefaultDataStrategy implements JacksonDataStrategy {
    
    @Override
    public String getId(ObjectNode doc) {
        return doc.get("_id").getTextValue();
    }
    
    @Override
    public String getTypeId(ObjectNode doc) {
        return doc.get(":type").getTextValue();
    }
    
    @Override
    public String getName(ObjectNode doc) {
        JsonNode name = doc.get(":name");
        return name == null ? null : name.getTextValue();
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

}
