package net.ontopia.presto.spi.rules;

import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeFlag;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class HasFieldValuesTypeRule extends BooleanTypeRule {

    @Override
    protected boolean getResult(TypeFlag flag, PrestoContext context, ObjectNode config) {
        if (context.isNewTopic()) {
            return false;
        } else {
            JsonNode fieldNode = config.path("field");
            if (fieldNode.isTextual()) {
                String fieldId = fieldNode.getTextValue();
                PrestoType type = context.getType();
                PrestoField field = type.getFieldById(fieldId);
                PrestoTopic topic = context.getTopic();
                List<? extends Object> values = topic.getValues(field);
                return !values.isEmpty();
            }
            return false;
        }
    }

}