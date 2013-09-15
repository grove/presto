package net.ontopia.presto.jaxrs.process.impl;

import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.process.ValueFactory;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoContextRules;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class NameMappingValueFactory extends ValueFactory {
    
    @Override
    public Value createValue(PrestoContextRules rules, PrestoFieldUsage field, String value) {
        Value result = new Value();
        result.setValue(value);
        result.setName(getName(rules, field, value));
        return result;
    }

    @Override
    public Value createValue(PrestoContextRules rules, PrestoFieldUsage field, PrestoTopic value) {
        Value result = new Value();
        result.setValue(value.getId());
        result.setName(getName(rules, field, value.getId()));
        return result;
    }

    private String getName(PrestoContextRules rules, PrestoFieldUsage field, String value) {
        ObjectNode config = getConfig();
        if (config != null) {
            JsonNode mappingNode = config.get("mapping");
            JsonNode mappedNode = mappingNode.path(value);
            if (mappedNode.isTextual()) {
                return mappedNode.getTextValue();
            }
        }
        return value;
    }

}
