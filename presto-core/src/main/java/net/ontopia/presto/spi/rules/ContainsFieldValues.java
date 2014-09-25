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
import net.ontopia.presto.spi.utils.PrestoContextRules;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ContainsFieldValues {

    public static boolean containsFieldValues(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, PrestoContextRules rules, ObjectNode config) {
        return containsFieldValues(dataProvider, schemaProvider, rules, null, config);
    }

    public static boolean containsFieldValues(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, PrestoContextRules rules, PrestoField defaultField, ObjectNode config) {
        List<? extends Object> values = HasFieldValues.getValues(dataProvider, schemaProvider, rules, defaultField, config);
        Set<String> configValues = getConfigValues(config);
        return ContainsFieldValues.containsAllValues(values, configValues);
    }

    public static Set<String> getConfigValues(ObjectNode config) {
        JsonNode valuesNode = config.path("values");
        if (valuesNode.isArray()) {
            Set<String> testValues = new LinkedHashSet<String>();
            for (JsonNode valueNode : valuesNode) {
                if (valueNode.isTextual()) {
                    testValues.add(valueNode.textValue());
                }
            }
            return testValues;
        }
        return Collections.emptySet();
    }

    public static boolean containsAllValues(Collection<? extends Object> values, Collection<? extends Object> configValues) {
        if (values.isEmpty()) {
            return configValues.isEmpty();
        }
        for (Object value : values) {
            String v;
            if (value instanceof PrestoTopic) {
                v = ((PrestoTopic)value).getId();
            } else {
                v = value.toString();
            }
            if (!configValues.contains(v)) {
                return false; 
            }
        }
        return true;
    }

}
