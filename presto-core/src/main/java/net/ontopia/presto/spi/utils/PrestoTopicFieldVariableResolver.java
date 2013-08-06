package net.ontopia.presto.spi.utils;

import java.util.ArrayList;
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
    public List<? extends Object> getValues(Object value, String variable) {
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
                return topic.getValues(valueField);
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
