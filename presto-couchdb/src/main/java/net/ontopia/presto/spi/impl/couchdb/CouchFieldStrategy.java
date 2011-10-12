package net.ontopia.presto.spi.impl.couchdb;

import net.ontopia.presto.spi.PrestoField;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public interface CouchFieldStrategy {

    ArrayNode getFieldValue(ObjectNode doc, PrestoField field);

    void putFieldValue(ObjectNode doc, PrestoField field, ArrayNode value);

}
