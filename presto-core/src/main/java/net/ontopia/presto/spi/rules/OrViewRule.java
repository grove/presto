package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules.ViewFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.ViewRule;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class OrViewRule extends BooleanViewRule {

    @Override
    protected boolean getResult(ViewFlag flag, PrestoContext context, PrestoView view, ObjectNode config) {
        if (config != null) {
            JsonNode handlers = config.path("handlers");
            if (!handlers.isMissingNode()) {
                for (ViewRule handler : AbstractHandler.getHandlers(getDataProvider(), getSchemaProvider(), ViewRule.class, handlers)) {
                    Boolean result = handler.getValue(flag, context, view);
                    if (result != null && result) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

}