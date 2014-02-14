package net.ontopia.presto.jaxrs.constraints;

import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.utils.PrestoContext;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public abstract class DefaultConstraintException extends ConstraintException {

    protected final PrestoSchemaProvider schemaProvider;
    protected PrestoContext context;
    protected PrestoFieldUsage field;

    protected DefaultConstraintException(PrestoSchemaProvider schemaProvider) {
        this.schemaProvider = schemaProvider;
    }
    
    protected DefaultConstraintException(PrestoContext context, PrestoFieldUsage field) {
        this.schemaProvider = field.getSchemaProvider();
        this.context = context;
        this.field = field;
    }

    protected PrestoSchemaProvider getSchemaProvider() {
        return schemaProvider;
    }

    protected PrestoContext getContext() {
        return context;
    }
    
    protected PrestoFieldUsage getField() {
        return field;
    }
    
    protected abstract String[] getMessageKeys();

    @Override
    public String getType() {
        return getMessageValue("type", "ALERT");
    }
    
    @Override
    public String getTitle() {
        return getMessageValue("title", "Constraint error");
    }
    
    @Override
    public String getMessage() {
        return getMessageValue("message", "Value is invalid.");
    }
    
    protected String getMessageValue(String property, String defaultValue) {
        for (String messageKey : getMessageKeys()) {
            for (ObjectNode extra : getExtraNodes()) {
                if (extra != null) {
                    ObjectNode messageNode = getMessageNodeFromExtraNode(messageKey, extra);
                    if (messageNode != null) {
                        String messageValue = messageNode.path(property).getTextValue();
                        if (messageValue != null) {
                            return messageValue;
                        }
                    }
                }
            }
        }
        return defaultValue;
    }

    protected ObjectNode[] getExtraNodes() {
        return new ObjectNode[] {
                field == null ? null : (ObjectNode)field.getExtra(),
                context == null ? null : (ObjectNode)context.getView().getExtra(),
                context == null ? null : (ObjectNode)context.getType().getExtra(),
                (ObjectNode)schemaProvider.getExtra()
        };
    }

    private ObjectNode getMessageNodeFromExtraNode(String messageKey, ObjectNode extra) {
        JsonNode resourcesNode = extra.path("error-messages");
        if (resourcesNode.isObject()) {
            JsonNode messageNode = resourcesNode.path(messageKey);
            if (messageNode.isObject()) {
                return (ObjectNode)messageNode;
            }
        }
        return null;
    }

}
