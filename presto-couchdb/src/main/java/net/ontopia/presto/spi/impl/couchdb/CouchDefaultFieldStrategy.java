package net.ontopia.presto.spi.impl.couchdb;

import net.ontopia.presto.spi.PrestoField;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public class CouchDefaultFieldStrategy implements CouchFieldStrategy {

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
