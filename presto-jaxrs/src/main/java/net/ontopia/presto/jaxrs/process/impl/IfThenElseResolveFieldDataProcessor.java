package net.ontopia.presto.jaxrs.process.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.PrestoContextRules;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.jaxrs.process.SubmittedState;
import net.ontopia.presto.jaxrs.resolve.PrestoTopicWithParentFieldVariableResolver;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.jackson.JacksonDataProvider;
import net.ontopia.presto.spi.utils.PrestoPaging;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public abstract class IfThenElseResolveFieldDataProcessor extends FieldDataProcessor {

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoContextRules rules, PrestoFieldUsage field) {
        if (isShouldRun(fieldData, rules, field)) {
            boolean result = getResult(fieldData, rules, field);
            if (result) {
                return thenProcessFieldData(fieldData, rules, field);
            } else {
                return elseProcessFieldData(fieldData, rules, field);
            }
        } else {
            return fieldData;
        }
    }

    protected boolean isShouldRun(FieldData fieldData, PrestoContextRules rules, PrestoFieldUsage field) {
        return true;
    }
    
    protected boolean getResult(FieldData fieldData, PrestoContextRules rules, PrestoFieldUsage field) {
        ObjectNode config = getConfig();
        if (config != null) {
            PrestoDataProvider dataProvider = getDataProvider();
    
            if (dataProvider instanceof JacksonDataProvider) {
    
                Paging paging = new PrestoPaging(0, 1);
    
                PrestoContext context = rules.getContext();
                PrestoVariableResolver parentResolver = new PrestoTopicWithParentFieldVariableResolver(field.getSchemaProvider(), context);
                PrestoVariableResolver variableResolver = new FieldDataVariableResolver(parentResolver, fieldData, context);
    
                PrestoTopic topic = context.getTopic();
    
                Collection<? extends Object> objects = (topic == null ? Collections.emptyList() : Collections.singleton(topic));
    
                JacksonDataProvider jacksonDataProvider = (JacksonDataProvider)dataProvider;
                JsonNode resolveConfig = config.path("resolve");
                PagedValues values = jacksonDataProvider.resolveValues(objects, field, paging, resolveConfig, variableResolver);
    
                return !values.getValues().isEmpty();
            }
        }
        return false;
    }

    protected FieldData thenProcessFieldData(FieldData fieldData, PrestoContextRules rules, PrestoFieldUsage field) {
        return fieldData;
    }

    protected FieldData elseProcessFieldData(FieldData fieldData, PrestoContextRules rules, PrestoFieldUsage field) {
        return fieldData;
    }

    private class FieldDataVariableResolver implements PrestoVariableResolver {

        private static final String SUBMITTED_PREFIX = ":submitted.";
        
        private final PrestoVariableResolver variableResolver;
        private final FieldData fieldData;
        private final PrestoContext context;

        public FieldDataVariableResolver(PrestoVariableResolver variableResolver, FieldData fieldData, PrestoContext context) {
            this.variableResolver = variableResolver;
            this.fieldData = fieldData;
            this.context = context;
        }

        @Override
        public List<? extends Object> getValues(Object value, String variable) {
            if (variable.equals(":value")) {
                return getFieldDataValues(fieldData);
            } else if (variable.equals(":value-if-new")) {
                if (context.isNewTopic()) {
                    return getFieldDataValues(fieldData);
                } else {
                    return Collections.emptyList();
                }
            } else if (variable.startsWith(SUBMITTED_PREFIX)) {
                SubmittedState submittedState = IfThenElseResolveFieldDataProcessor.this.getSubmittedState();
                if (submittedState != null) {
                    String fieldId = variable.substring(SUBMITTED_PREFIX.length());
                    return submittedState.getValues(fieldId);
                }
                return Collections.emptyList();
            } else {
                return variableResolver.getValues(value, variable);
            }
        }

        private List<String> getFieldDataValues(FieldData fieldData) {
            // TODO: should look up topics if reference field
            List<String> valueIds = new ArrayList<String>(); 
            for (Value value : fieldData.getValues()) {
                String valueId = value.getValue();
                valueIds.add(valueId);
            }
            return valueIds;
        }

    }

}
