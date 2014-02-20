package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldValueFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldValueRule;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class OrFieldValueRule extends BooleanFieldValueRule {

    @Override
    protected boolean getResult(FieldValueFlag flag, PrestoContext context, PrestoFieldUsage field, Object value, ObjectNode config) {
        if (config != null) {
            JsonNode handlers = config.path("handlers");
            if (!handlers.isMissingNode()) {
                for (FieldValueRule handler : AbstractHandler.getHandlers(getDataProvider(), getSchemaProvider(), FieldValueRule.class, handlers)) {
                    Boolean result = handler.getValue(flag, context, field, value);
                    if (result != null && result) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

}