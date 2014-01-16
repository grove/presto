package net.ontopia.presto.spi.rules;

import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.Utils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class HasFieldValues {

    public static boolean hasFieldValues(PrestoSchemaProvider schemaProvider, PrestoTopic topic, PrestoField defaultField, ObjectNode config) {
        PrestoField valueField = getValueField(schemaProvider, topic, config);
        if (valueField == null) {
            valueField = defaultField;
        }
        return hasFieldValues(topic, valueField);
    }
    
    public static boolean hasFieldValues(PrestoSchemaProvider schemaProvider, PrestoTopic topic, ObjectNode config) {
        PrestoField valueField = HasFieldValues.getValueField(schemaProvider, topic, config);
        if (valueField != null) {
            return HasFieldValues.hasFieldValues(topic, valueField);
        } else {
            return false;
        }
    }
    
    private static PrestoField getValueField(PrestoSchemaProvider schemaProvider, PrestoTopic topic, ObjectNode config) {
        JsonNode fieldNode = config.path("field");
        if (fieldNode.isTextual()) {
            String fieldId = fieldNode.getTextValue();
            PrestoType type = Utils.getTopicType(topic, schemaProvider);
            return type.getFieldById(fieldId);
        } else {
            return null;
        }
    }

    private static boolean hasFieldValues(PrestoTopic topic, PrestoField valueField) {
        List<? extends Object> values = topic.getValues(valueField);
        return !values.isEmpty();
    }

}
