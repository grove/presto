package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeRule;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class OrTypeRule extends BooleanTypeRule {

    @Override
    protected boolean getResult(TypeFlag flag, PrestoContext context, ObjectNode config) {
        if (config != null) {
            JsonNode handlers = config.path("handlers");
            if (!handlers.isMissingNode()) {
                for (TypeRule handler : AbstractHandler.getHandlers(getDataProvider(), getSchemaProvider(), TypeRule.class, handlers)) {
                    Boolean result = handler.getValue(flag, context);
                    if (result != null && result) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

}