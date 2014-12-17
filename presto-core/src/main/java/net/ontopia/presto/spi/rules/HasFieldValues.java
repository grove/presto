package net.ontopia.presto.spi.rules;

import java.util.List;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.utils.FieldValues;
import net.ontopia.presto.spi.utils.PrestoContextRules;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class HasFieldValues {
    
    public static boolean hasFieldValues(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, PrestoContextRules rules, ObjectNode config) {
        return hasFieldValues(dataProvider, schemaProvider, rules, null, config);
    }

    public static boolean hasFieldValues(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, PrestoContextRules rules, PrestoField defaultField, ObjectNode config) {
        List<? extends Object> values = getValues(dataProvider, schemaProvider, rules, defaultField, config);
        return !values.isEmpty();
    }
    
    static List<? extends Object> getValues(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, PrestoContextRules rules, PrestoField defaultField, ObjectNode config) {
        JsonNode fieldNode = config.path("field");
        if (fieldNode.isTextual()) {
            String fieldId = fieldNode.textValue();
            return PathExpressions.getValues(rules, fieldId);
        } else if (defaultField != null) {
            FieldValues fieldValues = rules.getFieldValues(defaultField);
            return fieldValues.getValues();
        }
        throw new RuntimeException("Not able to find field from configuration: " + config);
    }

}
