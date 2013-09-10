package net.ontopia.presto.jaxrs.rules;

import net.ontopia.presto.jaxrs.AbstractHandler;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.PrestoContextRules.FieldValueFlag;
import net.ontopia.presto.jaxrs.PrestoContextRules.FieldValueRule;
import net.ontopia.presto.spi.PrestoField;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public abstract class BooleanFieldValueRule extends AbstractHandler implements FieldValueRule {
    
    @Override
    public Boolean getValue(FieldValueFlag flag, PrestoContext context, PrestoField field, Object value) {
        ObjectNode config = getConfig();
        if (config != null) {
            boolean result = evaluateFieldValue(flag, context, field, value, config);
//            if (result) {
//                JsonNode valueNode = config.path("value");
//                return valueNode.isBoolean() ? valueNode.getBooleanValue() : true;
//            }
            return result;
        }
        return null;
    }

    protected boolean evaluateFieldValue(FieldValueFlag flag, PrestoContext context, PrestoField field, Object value, ObjectNode config) {
        return inverse(getResult(flag, context, field, value, config), config);
    }

    private boolean inverse(boolean value, ObjectNode config) {
        JsonNode inverseNode = config.path("inverse");
        if (inverseNode.isBoolean() && inverseNode.getBooleanValue()) {
            return !value;
        } else {
            return value;
        }
    }

    protected abstract boolean getResult(FieldValueFlag flag, PrestoContext context, PrestoField field, Object value, ObjectNode config);

}