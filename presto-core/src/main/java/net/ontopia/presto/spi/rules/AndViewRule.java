package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoContextRules.ViewFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.ViewRule;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class AndViewRule extends BooleanViewRule {

    @Override
    protected boolean getResult(ViewFlag flag, PrestoContextRules rules, PrestoView view, ObjectNode config) {
        if (config != null) {
            JsonNode handlers = config.path("handlers");
            if (!handlers.isMissingNode()) {
                for (ViewRule handler : AbstractHandler.getHandlers(getDataProvider(), getSchemaProvider(), ViewRule.class, handlers)) {
                    Boolean result = handler.getValue(flag, rules, view);
                    if (result != null && !result) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

}