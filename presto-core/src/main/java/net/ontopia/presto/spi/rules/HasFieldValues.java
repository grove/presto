package net.ontopia.presto.spi.rules;

import java.util.List;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoContext;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class HasFieldValues {
    
    public static boolean hasFieldValues(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, PrestoContext context, ObjectNode config) {
        return hasFieldValues(dataProvider, schemaProvider, context, null, config);
    }

    public static boolean hasFieldValues(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, PrestoContext context, PrestoField defaultField, ObjectNode config) {
        List<? extends Object> values = getValues(dataProvider, schemaProvider, context, defaultField, config);
        return !values.isEmpty();
    }
    
    static List<? extends Object> getValues(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, PrestoContext context, PrestoField defaultField, ObjectNode config) {
        JsonNode fieldNode = config.path("field");
        if (fieldNode.isTextual()) {
            String fieldId = fieldNode.getTextValue();
            return PathExpressions.getValues(dataProvider, schemaProvider, context, fieldId);
        } else if (defaultField != null) {
            PrestoTopic topic = context.getTopic();
            return topic.getValues(defaultField);
        }
        throw new RuntimeException("Not able to find field from configuration: " + config);
    }

}
