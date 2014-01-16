package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldValueFlag;

import org.codehaus.jackson.node.ObjectNode;

public class HasFieldValuesFieldValueRule extends BooleanFieldValueRule {

    @Override
    protected boolean getResult(FieldValueFlag flag, PrestoContext context, PrestoField field, Object value, ObjectNode config) {
        if (context.isNewTopic()) {
            return false;
        } else {
            if (value instanceof PrestoTopic) {
                PrestoTopic topic = (PrestoTopic)value;
                return HasFieldValues.hasFieldValues(getSchemaProvider(), topic, field, config);
            }
            return false;
        }
    }
    
}