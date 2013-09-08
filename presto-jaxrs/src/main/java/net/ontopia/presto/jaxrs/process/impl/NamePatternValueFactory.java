package net.ontopia.presto.jaxrs.process.impl;

import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.PrestoContextRules;
import net.ontopia.presto.jaxrs.process.ValueFactory;
import net.ontopia.presto.jaxrs.resolve.PrestoTopicWithParentFieldVariableResolver;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

public class NamePatternValueFactory extends ValueFactory {
    
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
        result.setName(getName(rules, field, value));
        return result;
    }

    private String getName(PrestoContextRules rules, PrestoFieldUsage field, String value) {
        PrestoContext context = rules.getContext();
        PrestoVariableResolver variableResolver = new PrestoTopicWithParentFieldVariableResolver(getSchemaProvider(), context);
        String name = PatternValueUtils.getValueByPattern(variableResolver, value, getConfig());
        if (name == null) {
            return value;
        } else {
            return name;
        }
    }

    private String getName(PrestoContextRules rules, PrestoFieldUsage field, PrestoTopic topic) {
        PrestoContext context = rules.getContext();
        PrestoVariableResolver variableResolver = new PrestoTopicWithParentFieldVariableResolver(getSchemaProvider(), context);
        String name = PatternValueUtils.getValueByPattern(variableResolver, topic, getConfig());
        if (name == null) {
            return topic.getName(field);
        } else {
            return name;
        }
    }

}
