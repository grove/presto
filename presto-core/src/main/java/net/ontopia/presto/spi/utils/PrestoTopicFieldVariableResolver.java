package net.ontopia.presto.spi.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

public class PrestoTopicFieldVariableResolver implements PrestoVariableResolver {

    private PrestoSchemaProvider schemaProvider;

    public PrestoTopicFieldVariableResolver(PrestoSchemaProvider schemaProvider) {
        this.schemaProvider = schemaProvider;
    }
    
    @Override
    public List<String> getValues(Object value, String variable) {
        if (value instanceof PrestoTopic) {
            List<String> result = new ArrayList<String>();
            PrestoTopic topic = (PrestoTopic)value;
            String typeId = topic.getTypeId();
            PrestoType type = schemaProvider.getTypeById(typeId);
            if (variable.equals(":id")) {
                result.add(topic.getId());                
            } else if (variable.equals(":name")) {
                result.add(topic.getName());                
            } else if (variable.equals(":type")) {
                result.add(typeId);                
            } else if (variable.equals(":type-name")) {
                result.add(type.getName());                
            } else {
                PrestoField valueField = type.getFieldById(variable);
                Collection<? extends Object> values = topic.getValues(valueField);
                for (Object v : values) {
                    if (v instanceof PrestoTopic) {
                        result.add((((PrestoTopic)v).getId()));
                    } else {
                        result.add(v == null ? null : v.toString());
                    }
                }
            }
            return result;
        } else {
            if (value != null && variable.equals(":value")) {
                return Collections.singletonList(value.toString()); 
            }
            return Collections.emptyList();
        }
    }

}
