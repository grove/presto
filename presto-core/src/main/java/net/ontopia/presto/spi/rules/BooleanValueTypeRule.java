package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeFlag;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class BooleanValueTypeRule extends BooleanTypeRule {

    @Override
    protected boolean getResult(TypeFlag flag, PrestoContext context, ObjectNode config) {
        JsonNode valueNode = config.path("value");
        return valueNode.isBoolean() ? valueNode.getBooleanValue() : true;
    }

}
