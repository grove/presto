package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoContextRules.ViewFlag;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class HasFieldValuesViewRule extends BooleanViewRule {

    @Override
    protected boolean getResult(ViewFlag flag, PrestoContextRules rules, PrestoView view, ObjectNode config) {
        return HasFieldValues.hasFieldValues(getDataProvider(), getSchemaProvider(), rules, config);
    }

}