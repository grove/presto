package net.ontopia.presto.spi.rules;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoContext;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class ContainsFieldValues {

    public static boolean containsFieldValues(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, PrestoContext context, ObjectNode config) {
        return containsFieldValues(dataProvider, schemaProvider, context, null, config);
    }

    public static boolean containsFieldValues(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, PrestoContext context, PrestoField defaultField, ObjectNode config) {
        List<? extends Object> values = HasFieldValues.getValues(dataProvider, schemaProvider, context, defaultField, config);
        Set<String> testValues = getTestValues(config);
        return ContainsFieldValues.containsAllValues(values, testValues);
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
