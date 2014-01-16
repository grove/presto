package net.ontopia.presto.spi.rules;

import java.util.List;
import java.util.Set;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldFlag;

import org.codehaus.jackson.node.ObjectNode;

public class ContainsFieldValuesFieldRule extends BooleanFieldRule {

    @Override
    protected boolean getResult(FieldFlag flag, PrestoContext context, PrestoField field, ObjectNode config) {
        if (context.isNewTopic()) {
            return false;
        } else {
            PrestoTopic topic = context.getTopic();
            return containsFieldValue(topic, field, config);
        }
    }

    private boolean containsFieldValue(PrestoTopic topic, PrestoField defaultField, ObjectNode config) {
        PrestoField valueField = ContainsFieldValues.getValueField(getSchemaProvider(), topic, config);
        if (valueField == null) {
            valueField = defaultField;
        }
        Set<String> testValues = ContainsFieldValues.getTestValues(config);
        List<? extends Object> fieldValues = topic.getValues(valueField);
        return ContainsFieldValues.containsAllValues(fieldValues, testValues);
    }
    
}