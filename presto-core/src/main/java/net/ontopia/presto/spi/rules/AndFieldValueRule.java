package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldValueFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldValueRule;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class AndFieldValueRule extends BooleanFieldValueRule {

    @Override
    protected boolean getResult(FieldValueFlag flag, PrestoContextRules rules, PrestoField field, Object value, ObjectNode config) {
        if (config != null) {
            JsonNode handlers = config.path("handlers");
            if (!handlers.isMissingNode()) {
                for (FieldValueRule handler : AbstractHandler.getHandlers(getDataProvider(), getSchemaProvider(), FieldValueRule.class, handlers)) {
                    Boolean result = handler.getValue(flag, rules, field, value);
                    if (result != null && !result) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

}