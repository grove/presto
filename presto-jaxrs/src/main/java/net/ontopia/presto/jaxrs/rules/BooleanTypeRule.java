package net.ontopia.presto.jaxrs.rules;

import net.ontopia.presto.jaxrs.AbstractHandler;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.PrestoContextRules.TypeFlag;
import net.ontopia.presto.jaxrs.PrestoContextRules.TypeRule;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public abstract class BooleanTypeRule extends AbstractHandler implements TypeRule {

    @Override
    public Boolean getValue(TypeFlag flag, PrestoContext context) {
        ObjectNode config = getConfig();
        if (config != null) {
            boolean result = evaluateType(flag, context, config);
            if (result) {
                JsonNode valueNode = config.path("value");
                return valueNode.isBoolean() ? valueNode.getBooleanValue() : true;
            }
        }
        return null;
    }
    
    protected boolean evaluateType(TypeFlag flag, PrestoContext context, ObjectNode config) {
        return inverse(getResult(flag, context, config), config);
    }

    private boolean inverse(boolean value, ObjectNode config) {
        JsonNode inverseNode = config.path("inverse");
        if (inverseNode.isBoolean() && inverseNode.getBooleanValue()) {
            return !value;
        } else {
            return value;
        }
    }
    
    protected abstract boolean getResult(TypeFlag flag, PrestoContext context, ObjectNode config);

}