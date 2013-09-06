package net.ontopia.presto.jaxrs.process.impl;

import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.process.ValueProcessor;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class ValueNameMappingProcessor extends ValueProcessor {

    @Override
    public String getName(PrestoContext context, PrestoFieldUsage field, String value) {
        return getMapping(context, field, value);
    }

    @Override
    public String getName(PrestoContext context, PrestoFieldUsage field, PrestoTopic topic) {
        return getMapping(context, field, topic.getId());
    }

    private String getMapping(PrestoContext context, PrestoFieldUsage field, String value) {
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
