package net.ontopia.presto.spi.utils;

import java.util.Collections;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.resolve.PrestoResolver;

public class PrestoTopicFieldVariableResolver implements PrestoVariableResolver {

    private final PrestoResolver resolver;

    public PrestoTopicFieldVariableResolver(PrestoResolver resolver) {
        this.resolver = resolver;
    }
    
    @Override
    public List<? extends Object> getValues(Object value, String variable) {
        if (value instanceof PrestoTopic) {
            PrestoTopic topic = (PrestoTopic)value;
            PrestoType type = Utils.getTopicType(topic, resolver.getSchemaProvider());
            if (variable.equals(":id")) {
                return Collections.singletonList(topic.getId());                
            } else if (variable.equals(":name")) {
                return Collections.singletonList(topic.getName());                
            } else if (variable.equals(":type")) {
                return Collections.singletonList(type.getId());                
            } else if (variable.equals(":type-name")) {
                return Collections.singletonList(type.getName());
            } else if (variable.startsWith("#")) {
                PrestoField valueField = type.getFieldById(variable.substring(1));
                return topic.getStoredValues(valueField);
            } else {
                PrestoField valueField = type.getFieldById(variable);
                return resolver.resolveValues(topic, valueField);
            }
        } else {
            if (value != null && variable.equals(":value")) {
                return Collections.singletonList(value.toString()); 
            }
            return Collections.emptyList();
        }
    }

}
