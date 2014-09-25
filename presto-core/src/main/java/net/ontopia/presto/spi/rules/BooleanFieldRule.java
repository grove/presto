package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldRule;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class BooleanFieldRule extends AbstractHandler implements FieldRule {

    @Override
    public Boolean getValue(FieldFlag flag, PrestoContextRules rules, PrestoField field) {
        ObjectNode config = getConfig();
        if (config != null) {
            return evaluateField(flag, rules, field, config);
        }
        return null;
    }

    protected boolean evaluateField(FieldFlag flag, PrestoContextRules rules, PrestoField field, ObjectNode config) {
        return inverse(getResult(flag, rules, field, config), config);
    }

    private boolean inverse(boolean value, ObjectNode config) {
        JsonNode inverseNode = config.path("inverse");
        if (inverseNode.isBoolean() && inverseNode.booleanValue()) {
            return !value;
        } else {
            return value;
        }
    }

    protected abstract boolean getResult(FieldFlag flag, PrestoContextRules rules, PrestoField field, ObjectNode config);

}