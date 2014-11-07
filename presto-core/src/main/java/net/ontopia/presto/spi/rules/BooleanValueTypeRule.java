package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeFlag;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class BooleanValueTypeRule extends BooleanTypeRule {

    @Override
    protected boolean getResult(TypeFlag flag, PrestoContextRules rules, ObjectNode config) {
        JsonNode valueNode = config.path("value");
        return valueNode.isBoolean() ? valueNode.booleanValue() : true;
    }

}
