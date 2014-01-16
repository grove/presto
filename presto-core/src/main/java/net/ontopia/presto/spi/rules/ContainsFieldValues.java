package net.ontopia.presto.spi.rules;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.Utils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class ContainsFieldValues {
    
    public static PrestoField getValueField(PrestoSchemaProvider schemaProvider, PrestoTopic topic, ObjectNode config) {
        JsonNode fieldNode = config.path("field");
        if (fieldNode.isTextual()) {
            String fieldId = fieldNode.getTextValue();
            PrestoType type = Utils.getTopicType(topic, schemaProvider);
            return type.getFieldById(fieldId);
        } else {
            return null;
        }
    }

    public static Set<String> getTestValues(ObjectNode config) {
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

    public static boolean containsAllValues(List<? extends Object> fieldValues, Collection<String> testValues) {
        if (fieldValues.isEmpty()) {
            return testValues.isEmpty();
        }
        for (Object value : fieldValues) {
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

}
