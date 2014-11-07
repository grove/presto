package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeFlag;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class HasFieldValuesTypeRule extends BooleanTypeRule {

    @Override
    protected boolean getResult(TypeFlag flag, PrestoContextRules rules, ObjectNode config) {
        return HasFieldValues.hasFieldValues(getDataProvider(), getSchemaProvider(), rules, config);
    }

}