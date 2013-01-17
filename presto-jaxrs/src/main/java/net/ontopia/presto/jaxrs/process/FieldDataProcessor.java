package net.ontopia.presto.jaxrs.process;

import java.text.MessageFormat;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;

public abstract class FieldDataProcessor extends AbstractProcessor {
    
    public abstract FieldData processFieldData(FieldData fieldData, PrestoTopic topic, PrestoFieldUsage field);

    protected String getErrorMessage(String errorId, PrestoFieldUsage field, String defaultErrorMessage, Object... args) {
        String errorMessage = getErrorMessage(errorId, field, defaultErrorMessage);
        if (args != null && args.length > 0) {
            return MessageFormat.format(errorMessage, args);
        } else {
            return errorMessage;
        }
    }
    
    protected String getErrorMessage(String errorId, PrestoFieldUsage field, String defaultErrorMessage) {
        ObjectNode extraNode = (ObjectNode)field.getExtra();
        if (extraNode != null) {
            JsonNode errorNode = extraNode.path("error-messages").path(errorId);
            if (errorNode.isTextual()) {
                return errorNode.getTextValue();
            }
        }
        return defaultErrorMessage;
    }

}
