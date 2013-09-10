package net.ontopia.presto.jaxrs.rules;

import java.util.List;

import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.PrestoContextRules.FieldValueFlag;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class HasFieldValuesFieldValueRule extends BooleanFieldValueRule {

    @Override
    protected boolean getResult(FieldValueFlag flag, PrestoContext context, PrestoField field, Object value, ObjectNode config) {
        if (context.isNewTopic()) {
            return false;
        } else {
            if (value instanceof PrestoTopic) {
                PrestoTopic topic = (PrestoTopic)value;
                JsonNode fieldNode = config.path("field");
                if (fieldNode.isTextual()) {
                    String typeId = topic.getTypeId();
                    PrestoSchemaProvider schemaProvider = field.getSchemaProvider();
                    PrestoType type = schemaProvider.getTypeById(typeId);
                    String fieldId = fieldNode.getTextValue();
                    PrestoField valueField = type.getFieldById(fieldId);
                    List<? extends Object> values = topic.getValues(valueField);
                    return !values.isEmpty();
                }
            }
            return false;
        }
    }

}