package net.ontopia.presto.jaxrs.rules;

import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.PrestoContextRules.ContextRulesHandler;
import net.ontopia.presto.jaxrs.PrestoContextRules.FieldFlag;
import net.ontopia.presto.jaxrs.PrestoContextRules.FieldRule;
import net.ontopia.presto.jaxrs.PrestoContextRules.FieldValueFlag;
import net.ontopia.presto.jaxrs.PrestoContextRules.FieldValueRule;
import net.ontopia.presto.jaxrs.PrestoContextRules.TypeFlag;
import net.ontopia.presto.jaxrs.PrestoContextRules.TypeRule;
import net.ontopia.presto.jaxrs.PrestoProcessor;
import net.ontopia.presto.spi.PrestoField;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class DelegatingContextRules extends ContextRulesHandler {

    @Override
    public Boolean getValue(TypeFlag flag, PrestoContext context) {
        JsonNode flagNode = getFlagNode(flag, context);
        if (flagNode != null && !flagNode.isMissingNode()) {
            for (TypeRule handler : PrestoProcessor.getHandlers(getPresto(), TypeRule.class, flagNode)) {
                Boolean result = handler.getValue(flag, context);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    @Override
    public Boolean getValue(FieldFlag flag, PrestoContext context, PrestoField field) {
        JsonNode flagNode = getFlagNode(flag, context, field);
        if (flagNode != null && !flagNode.isMissingNode()) {
            for (FieldRule handler : PrestoProcessor.getHandlers(getPresto(), FieldRule.class, flagNode)) {
                Boolean result = handler.getValue(flag, context, field);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    @Override
    public Boolean getValue(FieldValueFlag flag, PrestoContext context, PrestoField field, Object value) {
        JsonNode flagNode = getFlagNode(flag, context, field, value);
        if (flagNode != null && !flagNode.isMissingNode()) {
            for (FieldValueRule handler : PrestoProcessor.getHandlers(getPresto(), FieldValueRule.class, flagNode)) {
                Boolean result = handler.getValue(flag, context, field, value);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    protected JsonNode getFlagNode(TypeFlag flag, PrestoContext context) {
        ObjectNode config = getConfig();
        if (config != null) {
            return config.path(flag.name());
        }
        return null;
    }

    protected JsonNode getFlagNode(FieldFlag flag, PrestoContext context, PrestoField field) {
        // get config from field extra
        ObjectNode extra = (ObjectNode)field.getExtra();
        if (extra != null) {
            return extra.path("fieldRules").path(flag.name());
        }
        return null;
    }

    protected JsonNode getFlagNode(FieldValueFlag flag, PrestoContext context, PrestoField field, Object value) {
        // get config from field extra
        ObjectNode extra = (ObjectNode)field.getExtra();
        if (extra != null) {
            return extra.path("fieldValueRules").path(flag.name());
        }
        return null;
    }
    
}