package net.ontopia.presto.jaxrs.process.impl;

import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.process.ValueProcessor;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoTopicFieldVariableResolver;

public class ValueNamePatternProcessor extends ValueProcessor {

    @Override
    public String getName(PrestoContext context, PrestoFieldUsage field, String value) {
        return value;
    }

    @Override
    public String getName(PrestoContext context, PrestoFieldUsage field, PrestoTopic topic) {
        PrestoTopicFieldVariableResolver variableResolver = new PrestoTopicFieldVariableResolver(getSchemaProvider());
        String name = PatternValueUtils.getValueByPattern(variableResolver, topic, getConfig());
        if (name == null) {
            name = topic.getName(field);
        }
        return name;
    }

}
