package net.ontopia.presto.jaxrs.rules;

import net.ontopia.presto.jaxrs.AbstractHandler;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.PrestoContextRules.FieldFlag;
import net.ontopia.presto.jaxrs.PrestoContextRules.FieldRule;
import net.ontopia.presto.spi.PrestoField;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public abstract class BooleanFieldRule extends AbstractHandler implements FieldRule {

    @Override
    public Boolean getValue(FieldFlag flag, PrestoContext context, PrestoField field) {
        ObjectNode config = getConfig();
        if (config != null) {
            boolean result = evaluateField(flag, context, field, config);
            if (result) {
                JsonNode valueNode = config.path("value");
                return valueNode.isBoolean() ? valueNode.getBooleanValue() : true;
            }
        }
        return null;
    }

    protected boolean evaluateField(FieldFlag flag, PrestoContext context, PrestoField field, ObjectNode config) {
        return inverse(getResult(flag, context, field, config), config);
    }

    private boolean inverse(boolean value, ObjectNode config) {
        JsonNode inverseNode = config.path("inverse");
        if (inverseNode.isBoolean() && inverseNode.getBooleanValue()) {
            return !value;
        } else {
            return value;
        }
    }

    protected abstract boolean getResult(FieldFlag flag, PrestoContext context, PrestoField field, ObjectNode config);

}