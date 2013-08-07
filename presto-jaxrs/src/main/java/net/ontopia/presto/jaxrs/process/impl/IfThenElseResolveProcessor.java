package net.ontopia.presto.jaxrs.process.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
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

public abstract class IfThenElseResolveProcessor extends FieldDataProcessor {

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoContext context, PrestoFieldUsage field) {
        ObjectNode processorConfig = getConfig();
        if (processorConfig != null) {
            JsonNode resolveConfig = processorConfig.path("resolve");
            PrestoDataProvider dataProvider = getDataProvider();

            if (dataProvider instanceof JacksonDataProvider) {
                Paging paging = new PrestoPaging(0, 1);
                
                PrestoVariableResolver parentResolver = new PrestoTopicWithParentFieldVariableResolver(field.getSchemaProvider(), context);
                PrestoVariableResolver variableResolver = new FieldDataVariableResolver(parentResolver, fieldData, context);
                
                PrestoTopic topic = context.getTopic();
                
                Collection<? extends Object> objects = (topic == null ? Collections.emptyList() : Collections.singleton(topic));
                
                JacksonDataProvider jacksonDataProvider = (JacksonDataProvider)dataProvider;
                PagedValues result = jacksonDataProvider.resolveValues(objects, field, paging, resolveConfig, variableResolver);

                if (result.getValues().size() > 0) {
                    return thenProcessFieldData(fieldData, context, field);
                } else {
                    return elseProcessFieldData(fieldData, context, field);
                }
            }
        }
        return fieldData;
    }

    protected FieldData thenProcessFieldData(FieldData fieldData, PrestoContext context, PrestoFieldUsage field) {
        return fieldData;
    }

    protected FieldData elseProcessFieldData(FieldData fieldData, PrestoContext context, PrestoFieldUsage field) {
        return fieldData;
    }
    
    private static class FieldDataVariableResolver implements PrestoVariableResolver {

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
