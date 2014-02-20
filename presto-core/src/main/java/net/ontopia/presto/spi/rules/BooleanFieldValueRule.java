package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldValueFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldValueRule;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public abstract class BooleanFieldValueRule extends AbstractHandler implements FieldValueRule {
    
    @Override
    public Boolean getValue(FieldValueFlag flag, PrestoContext context, PrestoField field, Object value) {
        ObjectNode config = getConfig();
        if (config != null) {
            return evaluateFieldValue(flag, context, field, value, config);
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