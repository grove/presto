package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules.ViewFlag;

import org.codehaus.jackson.node.ObjectNode;

public class HasFieldValuesViewRule extends BooleanViewRule {

    @Override
    protected boolean getResult(ViewFlag flag, PrestoContext context, PrestoView view, ObjectNode config) {
        if (context.isNewTopic()) {
            return false;
        } else {
            PrestoTopic topic = context.getTopic();
            return HasFieldValues.hasFieldValues(getSchemaProvider(), topic, config);
        }
    }

}