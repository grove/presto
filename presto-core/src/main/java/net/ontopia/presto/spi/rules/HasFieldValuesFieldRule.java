package net.ontopia.presto.spi.rules;

import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldFlag;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class HasFieldValuesFieldRule extends BooleanFieldRule {

    @Override
    protected boolean getResult(FieldFlag flag, PrestoContext context, PrestoField field, ObjectNode config) {
        if (context.isNewTopic()) {
            return false;
        } else {
            PrestoTopic topic = context.getTopic();
            PrestoField valueField = getValueField(context, field, config);
            List<? extends Object> values = topic.getValues(valueField);
            return !values.isEmpty();
        }
    }

    private PrestoField getValueField(PrestoContext context, PrestoField field, ObjectNode config) {
        JsonNode fieldNode = config.path("field");
        if (fieldNode.isTextual()) {
            String fieldId = fieldNode.getTextValue();
            PrestoType type = context.getType();
            return type.getFieldById(fieldId);
        } else {
            return field;
        }
    }

}