package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeRule;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AndTypeRule extends BooleanTypeRule {

    @Override
    protected boolean getResult(TypeFlag flag, PrestoContextRules rules, ObjectNode config) {
        if (config != null) {
            JsonNode handlers = config.path("handlers");
            if (!handlers.isMissingNode()) {
                for (TypeRule handler : AbstractHandler.getHandlers(getDataProvider(), getSchemaProvider(), TypeRule.class, handlers)) {
                    Boolean result = handler.getValue(flag, rules);
                    if (result != null && !result) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

}