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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainsFieldValues {
    
    private static Logger log = LoggerFactory.getLogger(ContainsFieldValues.class);

    public static boolean containsFieldValue(PrestoSchemaProvider schemaProvider, PrestoTopic topic, PrestoField defaultField, ObjectNode config) {
        PrestoField valueField = getValueField(schemaProvider, topic, config);
        if (valueField == null) {
            valueField = defaultField;
        }
        Set<String> testValues = getTestValues(config);
        List<? extends Object> fieldValues = topic.getValues(valueField);
        return ContainsFieldValues.containsAllValues(fieldValues, testValues);
    }

    public static boolean containsFieldValue(PrestoSchemaProvider schemaProvider, PrestoTopic topic, ObjectNode config) {
        PrestoField valueField = ContainsFieldValues.getValueField(schemaProvider, topic, config);
        if (valueField != null) {
            Set<String> testValues = ContainsFieldValues.getTestValues(config);
            List<? extends Object> fieldValues = topic.getValues(valueField);
            return ContainsFieldValues.containsAllValues(fieldValues, testValues);
        } else {
            log.warn("Not able to find field from configuration: " + config);
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

    private static Set<String> getTestValues(ObjectNode config) {
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

    private static boolean containsAllValues(List<? extends Object> fieldValues, Collection<String> testValues) {
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
