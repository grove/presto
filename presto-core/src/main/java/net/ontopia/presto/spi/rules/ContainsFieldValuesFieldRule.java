package net.ontopia.presto.spi.rules;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldFlag;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class ContainsFieldValuesFieldRule extends BooleanFieldRule {

    @Override
    protected boolean getResult(FieldFlag flag, PrestoContext context, PrestoField field, ObjectNode config) {
        if (context.isNewTopic()) {
            return false;
        } else {
            PrestoTopic topic = context.getTopic();
            PrestoField valueField = getValueField(context, field, config);
            return isTrue(config, topic, valueField);
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

    protected boolean isTrue(ObjectNode config, PrestoTopic topic, PrestoField valueField) {
        Set<String> testValues = getTestValues(config);
        List<? extends Object> values = topic.getValues(valueField);
        for (Object value : values) {
            String v;
            if (value instanceof PrestoTopic) {
                v = ((PrestoTopic)value).getId();
            } else {
                v = value.toString();
            }
            if (!testValues.contains(v)) {
                return false; 
            }
        }
        return true;
    }

    private Set<String>getTestValues(ObjectNode config) {
        JsonNode valuesNode = config.path("values");
        if (valuesNode.isArray()) {
            Set<String> testValues = new LinkedHashSet<String>();
            for (JsonNode valueNode : valuesNode) {
                if (valueNode.isTextual()) {
                    testValues.add(valueNode.getTextValue());
                }
            }
            return testValues;
        }
        return Collections.emptySet();
    }
    
}