package net.ontopia.presto.spi.jackson;

import net.ontopia.presto.spi.PrestoField;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public interface JacksonDataStrategy {

    String getId(ObjectNode doc);

    String getTypeId(ObjectNode doc);

    String getName(ObjectNode doc);

    String getName(ObjectNode doc, PrestoField field);

    boolean hasFieldValue(ObjectNode data, PrestoField field);
    
    ArrayNode getFieldValue(ObjectNode doc, PrestoField field);

    void putFieldValue(ObjectNode doc, PrestoField field, ArrayNode value);

    void clearFieldValue(ObjectNode doc, PrestoField field);

}
