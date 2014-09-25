package net.ontopia.presto.jaxrs.process;

import java.text.MessageFormat;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.utils.PrestoContextRules;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class FieldDataProcessor extends AbstractProcessor {
    
    private SubmittedState sstate;
    
    public abstract FieldData processFieldData(FieldData fieldData, PrestoContextRules context, PrestoField field, Projection projection);

    protected String getErrorMessage(String errorId, PrestoField field, String defaultErrorMessage, Object... args) {
        String errorMessage = getErrorMessageConfig(errorId, field, defaultErrorMessage);
        if (args != null && args.length > 0) {
            return MessageFormat.format(errorMessage, args);
        } else {
            return errorMessage;
        }
    }
    
    private String getErrorMessageConfig(String errorId, PrestoField field, String defaultErrorMessage) {
        ObjectNode config = getConfig();
        if (config != null) {
            JsonNode errorNode = config.path("error-messages").path(errorId);
            if (errorNode.isTextual()) {
                return errorNode.textValue();
            }
        }
        return defaultErrorMessage;
    }

    protected SubmittedState getSubmittedState() {
        return sstate;
    }
    
    public void setSubmittedState(SubmittedState sstate) {
        this.sstate = sstate;
    }
    
}
