package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeRule;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class BooleanTypeRule extends AbstractHandler implements TypeRule {

    @Override
    public Boolean getValue(TypeFlag flag, PrestoContextRules rules) {
        ObjectNode config = getConfig();
        if (config != null) {
            return evaluateType(flag, rules, config);
        }
        return null;
    }
    
    protected boolean evaluateType(TypeFlag flag, PrestoContextRules rules, ObjectNode config) {
        return inverse(getResult(flag, rules, config), config);
    }

    private boolean inverse(boolean value, ObjectNode config) {
        JsonNode inverseNode = config.path("inverse");
        if (inverseNode.isBoolean() && inverseNode.booleanValue()) {
            return !value;
        } else {
            return value;
        }
    }
    
    protected abstract boolean getResult(TypeFlag flag, PrestoContextRules rules, ObjectNode config);

}