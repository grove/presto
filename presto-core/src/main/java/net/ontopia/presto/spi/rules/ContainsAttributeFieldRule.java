package net.ontopia.presto.spi.rules;

import java.util.Collections;
import java.util.Set;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldFlag;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class ContainsAttributeFieldRule extends BooleanFieldRule {

    @Override
    protected boolean getResult(FieldFlag flag, PrestoContextRules rules, PrestoField field, ObjectNode config) {
        return isAttributeEquals(rules, config);
    }

    private boolean isAttributeEquals(PrestoContextRules rules, ObjectNode config) {
        JsonNode fieldNode = config.path("attribute");
        if (fieldNode.isTextual()) {
            String name = fieldNode.getTextValue();
            Object value = rules.getAttributes().getAttribute(name);
            Set<String> configValues = ContainsFieldValues.getConfigValues(config);
            return ContainsFieldValues.containsAllValues(Collections.singletonList(value), configValues);
        }
        return false;
    }

}