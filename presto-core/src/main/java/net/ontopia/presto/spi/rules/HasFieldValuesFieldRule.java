package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldFlag;

import org.codehaus.jackson.node.ObjectNode;

public class HasFieldValuesFieldRule extends BooleanFieldRule {

    @Override
    protected boolean getResult(FieldFlag flag, PrestoContext context, PrestoField field, ObjectNode config) {
        if (context.isNewTopic()) {
            return false;
        } else {
            return HasFieldValues.hasFieldValues(getDataProvider(), getSchemaProvider(), context, field, config);
        }
    }

}