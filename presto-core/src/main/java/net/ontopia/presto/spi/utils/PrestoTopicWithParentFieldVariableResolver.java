package net.ontopia.presto.spi.utils;

import java.util.List;

import net.ontopia.presto.spi.PrestoSchemaProvider;

public class PrestoTopicWithParentFieldVariableResolver extends PrestoTopicFieldVariableResolver {

    private static final String PARENT_PREFIX = ":parent.";
    
    private final PrestoContext context;

    public PrestoTopicWithParentFieldVariableResolver(PrestoSchemaProvider schemaProvider, PrestoContext context) {
        super(schemaProvider);
        this.context = context;
    }
    
    @Override
    public List<? extends Object> getValues(Object value, String variable) {
        if (variable.startsWith(PARENT_PREFIX)) {
            String parentVariable = getParentVariable(context, variable);
            Object parentTopic = getParentTopic(context, variable);
            return super.getValues(parentTopic, parentVariable);
        } else {
            return super.getValues(value, variable);
        }
    }
    
    private Object getParentTopic(PrestoContext parentContext, String variable) {
        if (variable.startsWith(PARENT_PREFIX)) {
            String parentVariable = variable.substring(PARENT_PREFIX.length());
            if (parentVariable.startsWith(PARENT_PREFIX)) {
                return getParentTopic(parentContext.getParentContext(), parentVariable);
            } else {
                return parentContext.getTopic();
            }
        } else {
            return parentContext.getTopic();
        }
    }

    private String getParentVariable(PrestoContext parentContext, String variable) {
        if (variable.startsWith(PARENT_PREFIX)) {
            String parentVariable = variable.substring(PARENT_PREFIX.length());
            return getParentVariable(parentContext.getParentContext(), parentVariable);
        } else {
            return variable;
        }
    }
    
}
