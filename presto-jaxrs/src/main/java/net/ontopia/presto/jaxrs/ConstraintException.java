package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoSchemaProvider;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public abstract class ConstraintException extends RuntimeException {

    public ConstraintException() {
    }
    
    public ConstraintException(String message) {
        super(message);
    }

    public abstract String getType();
    
    public abstract String getTitle();

    protected String getType(PrestoSchemaProvider schemaProvider, String messageKey) {
        ObjectNode messageNode = getMessageNode(schemaProvider, messageKey);
        return messageNode != null ? messageNode.path("type").getTextValue() : null;
    }

    protected String getTitle(PrestoSchemaProvider schemaProvider, String messageKey) {
        ObjectNode messageNode = getMessageNode(schemaProvider, messageKey);
        return messageNode != null ? messageNode.path("title").getTextValue() : null;
    }

    protected String getMessage(PrestoSchemaProvider schemaProvider, String messageKey) {
        ObjectNode messageNode = getMessageNode(schemaProvider, messageKey);
        return messageNode != null ? messageNode.path("message").getTextValue() : null;
    }

    protected ObjectNode getMessageNode(PrestoSchemaProvider schemaProvider, String messageKey) {
        ObjectNode extra = (ObjectNode)schemaProvider.getExtra();
        if (extra != null) {
            JsonNode resourcesNode = extra.path("messages");
            if (resourcesNode.isObject()) {
                JsonNode messageNode = resourcesNode.path(messageKey);
                if (messageNode.isObject()) {
                    return (ObjectNode)messageNode;
                }
            }
        }
        return null;
    }
    
}
