package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeRule;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public abstract class BooleanTypeRule extends AbstractHandler implements TypeRule {

    @Override
    public Boolean getValue(TypeFlag flag, PrestoContext context) {
        ObjectNode config = getConfig();
        if (config != null) {
            return evaluateType(flag, context, config);
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