package net.ontopia.presto.spi.jackson;

import net.ontopia.presto.spi.PrestoField;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public interface JacksonFieldDataStrategy {

    void setJacksonDataStrategy(JacksonDataStrategy dataStrategy);
    
    ArrayNode getFieldValue(ObjectNode doc, PrestoField field);

}
