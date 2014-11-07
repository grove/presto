package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldRule;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class OrFieldRule extends BooleanFieldRule {

    @Override
    protected boolean getResult(FieldFlag flag, PrestoContextRules rules, PrestoField field, ObjectNode config) {
        if (config != null) {
            JsonNode handlers = config.path("handlers");
            if (!handlers.isMissingNode()) {
                for (FieldRule handler : AbstractHandler.getHandlers(getDataProvider(), getSchemaProvider(), FieldRule.class, handlers)) {
                    Boolean result = handler.getValue(flag, rules, field);
                    if (result != null && result) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

}