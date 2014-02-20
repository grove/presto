package net.ontopia.presto.jaxrs.process.impl;

import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.process.ValueFactory;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PatternValueUtils;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoTopicWithParentFieldVariableResolver;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

public class NamePatternValueFactory extends ValueFactory {
    
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
        result.setName(getName(rules, field, value));
        return result;
    }

    private String getName(PrestoContextRules rules, PrestoField field, String value) {
        PrestoContext context = rules.getContext();
        PrestoVariableResolver variableResolver = new PrestoTopicWithParentFieldVariableResolver(getSchemaProvider(), context);
        String name = PatternValueUtils.getValueByPattern(variableResolver, value, getConfig());
        if (name == null) {
            return value;
        } else {
            return name;
        }
    }

    private String getName(PrestoContextRules rules, PrestoField field, PrestoTopic topic) {
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
