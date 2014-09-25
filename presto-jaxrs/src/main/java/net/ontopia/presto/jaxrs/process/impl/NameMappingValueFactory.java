package net.ontopia.presto.jaxrs.process.impl;

import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.process.ValueFactory;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoContextRules;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class NameMappingValueFactory extends ValueFactory {
    
    @Override
    public Value createValue(PrestoContextRules rules, PrestoField field, String value) {
        Value result = new Value();
        result.setValue(value);
        result.setName(getName(rules, field, value));
        return result;
    }

    @Override
    public Value createValue(PrestoContextRules rules, PrestoField field, PrestoTopic value) {
        Value result = new Value();
        result.setValue(value.getId());
        result.setName(getName(rules, field, value.getId()));
        return result;
    }

    private String getName(PrestoContextRules rules, PrestoField field, String value) {
        ObjectNode config = getConfig();
        if (config != null) {
            JsonNode mappingNode = config.get("mapping");
            JsonNode mappedNode = mappingNode.path(value);
            if (mappedNode.isTextual()) {
                return mappedNode.textValue();
            }
        }
        return value;
    }

}
