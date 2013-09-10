package net.ontopia.presto.jaxrs.rules;

import java.util.List;

import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.PrestoContextRules.FieldFlag;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class HasFieldValuesFieldRule extends BooleanFieldRule {

    @Override
    protected boolean getResult(FieldFlag flag, PrestoContext context, PrestoField field, ObjectNode config) {
        if (context.isNewTopic()) {
            return false;
        } else {
            PrestoTopic topic = context.getTopic();
            JsonNode fieldNode = config.path("field");
            PrestoField valueField;
            if (fieldNode.isTextual()) {
                String fieldId = fieldNode.getTextValue();
                PrestoType type = context.getType();
                valueField = type.getFieldById(fieldId);
            } else {
                valueField = field;
            }
            List<? extends Object> values = topic.getValues(valueField);
            return !values.isEmpty();
        }
    }

}