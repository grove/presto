package net.ontopia.presto.jaxrs.resolve;

import java.util.Collection;
import java.util.List;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.rules.PathExpressions;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class ExpressionAvailableFieldValuesResolver extends AvailableFieldValuesResolver {

    @Override
    public Collection<? extends Object> getAvailableFieldValues(PrestoContextRules rules, PrestoField field, String query) {
        PrestoDataProvider dataProvider = getDataProvider();
        return getValues(dataProvider, getSchemaProvider(), rules, getConfig());
    }

    private List<? extends Object> getValues(PrestoDataProvider dataProvider, 
            PrestoSchemaProvider schemaProvider, PrestoContextRules rules, ObjectNode config) {

        JsonNode fieldNode = config.path("field");
        if (fieldNode.isTextual()) {
            PrestoContext context = rules.getContext();
            PrestoContext parentContext = context.getParentContext();
            PrestoContextRules parentRules = rules.getPrestoContextRules(parentContext);
            String fieldId = fieldNode.getTextValue();
            return PathExpressions.getValues(dataProvider, schemaProvider, parentRules, fieldId);
        } 
        throw new RuntimeException("Not able to find field from configuration: " + config);
    }

}
