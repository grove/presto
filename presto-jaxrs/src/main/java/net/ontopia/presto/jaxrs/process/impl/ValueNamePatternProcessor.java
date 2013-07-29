package net.ontopia.presto.jaxrs.process.impl;

import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.process.ValueProcessor;
import net.ontopia.presto.jaxrs.resolve.PrestoTopicWithParentFieldVariableResolver;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

public class ValueNamePatternProcessor extends ValueProcessor {

    @Override
    public String getName(PrestoContext context, PrestoFieldUsage field, String value) {
        PrestoVariableResolver variableResolver = new PrestoTopicWithParentFieldVariableResolver(getSchemaProvider(), context);
        String name = PatternValueUtils.getValueByPattern(variableResolver, value, getConfig());
        if (name == null) {
            return value;
        } else {
            return name;
        }
    }

    @Override
    public String getName(PrestoContext context, PrestoFieldUsage field, PrestoTopic topic) {
        PrestoVariableResolver variableResolver = new PrestoTopicWithParentFieldVariableResolver(getSchemaProvider(), context);
        String name = PatternValueUtils.getValueByPattern(variableResolver, topic, getConfig());
        if (name == null) {
            return topic.getName(field);
        } else {
            return name;
        }
    }

}
