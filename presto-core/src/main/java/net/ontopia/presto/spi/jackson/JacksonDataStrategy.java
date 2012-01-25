package net.ontopia.presto.spi.jackson;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoFieldUsage;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public interface JacksonDataStrategy {

    String getId(ObjectNode doc);

    String getTypeId(ObjectNode doc);

    String getName(ObjectNode doc);

    String getName(ObjectNode doc, PrestoFieldUsage field);
    
    ArrayNode getFieldValue(ObjectNode doc, PrestoField field);

    void putFieldValue(ObjectNode doc, PrestoField field, ArrayNode value);

}
