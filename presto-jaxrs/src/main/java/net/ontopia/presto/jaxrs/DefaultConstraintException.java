package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoSchemaProvider;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public abstract class DefaultConstraintException extends ConstraintException {

    private final PrestoSchemaProvider schemaProvider;

    protected DefaultConstraintException(PrestoSchemaProvider schemaProvider) {
        this.schemaProvider = schemaProvider;
    }

    protected PrestoSchemaProvider getSchemaProvider() {
        return schemaProvider;
    }

    protected abstract String getMessageKey();

    @Override
    public String getType() {
        String type = getType(getSchemaProvider(), getMessageKey());
        return type != null ? type : "ALERT";
    }
    
    @Override
    public String getTitle() {
        String title = getTitle(getSchemaProvider(), getMessageKey());
        return title != null ? title : "Constraint error";
    }
    
    @Override
    public String getMessage() {
        String message = getMessage(getSchemaProvider(), getMessageKey());
        return message != null ? message : "Value is invalid.";
    }

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
