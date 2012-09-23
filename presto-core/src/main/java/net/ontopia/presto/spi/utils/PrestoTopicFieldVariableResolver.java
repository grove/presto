package net.ontopia.presto.spi.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

public class PrestoTopicFieldVariableResolver implements PrestoVariableResolver {

    private final PrestoVariableContext context;

    public PrestoTopicFieldVariableResolver(PrestoVariableContext context) {
        this.context = context;
    }
    
    @Override
    public List<String> getValues(Object value, String variable) {
        if (value instanceof PrestoTopic) {
            List<String> result = new ArrayList<String>();
            PrestoTopic topic = (PrestoTopic)value;
            String typeId = topic.getTypeId();
            PrestoType type = context.getSchemaProvider().getTypeById(typeId);
            if (variable.equals(":id")) {
                result.add(topic.getId());                
            } else if (variable.equals(":type")) {
                result.add(topic.getTypeId());                
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
            return Collections.emptyList();
        }
    }

}
