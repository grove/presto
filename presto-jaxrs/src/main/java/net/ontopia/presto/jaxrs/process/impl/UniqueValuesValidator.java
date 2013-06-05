package net.ontopia.presto.jaxrs.process.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.jackson.JacksonDataProvider;
import net.ontopia.presto.spi.utils.PrestoPaging;
import net.ontopia.presto.spi.utils.PrestoTopicFieldVariableResolver;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class UniqueValuesValidator extends FieldDataProcessor {

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoContext context, PrestoFieldUsage field) {
        ObjectNode processorConfig = getConfig();
        if (processorConfig != null) {
            JsonNode resolveConfig = processorConfig.path("resolve");
            PrestoDataProvider dataProvider = getDataProvider();

            if (dataProvider instanceof JacksonDataProvider) {
                JacksonDataProvider jacksonDataProvider = (JacksonDataProvider)dataProvider;
                Paging paging = new PrestoPaging(0, 1);
                
                PrestoVariableResolver variableResolver = new FieldDataVariableResolver(field.getSchemaProvider(), fieldData);
                
                PrestoTopic topic = context.getTopic();
                
                Collection<? extends Object> objects = (topic == null ? Collections.emptyList() : Collections.singleton(topic));
                
                PagedValues result = jacksonDataProvider.resolveValues(objects, field, paging, resolveConfig, variableResolver);
                if (result.getValues().size() > 0) {
                    setValid(false);
                    addError(fieldData, getErrorMessage("not-unique", field, "Field value is not unique."));
                }
            }
        }
        return fieldData;
    }

    public class FieldDataVariableResolver implements PrestoVariableResolver {

        private final PrestoVariableResolver variableResolver;
        private final FieldData fieldData;

        FieldDataVariableResolver(PrestoSchemaProvider schemaProvider, FieldData fieldData) {
            this.variableResolver = new PrestoTopicFieldVariableResolver(schemaProvider);
            this.fieldData = fieldData;
        }

        @Override
        public List<String> getValues(Object value, String variable) {
            if (variable.equals(":value")) {
                return getFieldDataValues(fieldData);
            } else {
                return variableResolver.getValues(value, variable);
            }
        }

        private List<String> getFieldDataValues(FieldData fieldData) {
            List<String> valueIds = new ArrayList<String>(); 
            for (Value value : fieldData.getValues()) {
                String valueId = value.getValue();
                valueIds.add(valueId);
            }
            return valueIds;
        }
        
    }
    
}
