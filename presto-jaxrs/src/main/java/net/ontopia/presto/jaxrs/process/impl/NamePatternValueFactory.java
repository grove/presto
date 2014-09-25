package net.ontopia.presto.jaxrs.process.impl;

import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.process.ValueFactory;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PatternValueUtils;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoTopicWithParentFieldVariableResolver;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class NamePatternValueFactory extends ValueFactory {
    
    @Override
    public Value createValue(PrestoContextRules rules, PrestoField field, String value) {
        Value result = new Value();
        result.setValue(value);
        result.setName(getName(getSchemaProvider(), rules, field, value, getConfig()));
        return result;
    }

    @Override
    public Value createValue(PrestoContextRules rules, PrestoField field, PrestoTopic value) {
        Value result = new Value();
        result.setValue(value.getId());
        result.setName(getName(getSchemaProvider(), rules, field, value, getConfig()));
        return result;
    }

    static String getName(PrestoSchemaProvider schemaProvider, PrestoContextRules rules, PrestoField field, String value, ObjectNode config) {
        PrestoContext context = rules.getContext();
        PrestoVariableResolver variableResolver = new PrestoTopicWithParentFieldVariableResolver(schemaProvider, context);
        String name = PatternValueUtils.getValueByPattern(variableResolver, value, config);
        if (name == null) {
            return value;
        } else {
            return name;
        }
    }

    static String getName(PrestoSchemaProvider schemaProvider, PrestoContextRules rules, PrestoField field, PrestoTopic topic, ObjectNode config) {
        PrestoContext context = rules.getContext();
        PrestoVariableResolver variableResolver = new PrestoTopicWithParentFieldVariableResolver(schemaProvider, context);
        String name = PatternValueUtils.getValueByPattern(variableResolver, topic, config);
        if (name == null) {
            return topic.getName(field);
        } else {
            return name;
        }
    }

}
