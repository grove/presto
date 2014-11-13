package net.ontopia.presto.jaxrs.resolve;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.jackson.JacksonDataProvider;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoTopicFieldVariableResolver;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DefaultAvailableFieldValuesResolver extends AvailableFieldValuesResolver {

    @Override
    public Collection<? extends Object> getAvailableFieldValues(PrestoContextRules rules, PrestoField field, String query) {
        ObjectNode processorConfig = getConfig();
        if (processorConfig != null) {
            JsonNode resolveConfig = processorConfig.path("resolve");
            PrestoDataProvider dataProvider = getDataProvider();

            if (dataProvider instanceof JacksonDataProvider) {
                Projection projection = null; // new PrestoPaging(0, 100);
                
                PrestoContext context = rules.getContext();
                PrestoVariableResolver variableResolver = new QueryFilterVariableResolver(context, query);
                
                PrestoTopic topic = context.getTopic();
                
                Collection<? extends Object> objects = (topic == null ? Collections.emptyList() : Collections.singleton(topic));
                
                PagedValues result = context.resolveValues(objects, field, projection, resolveConfig, variableResolver);
                return result.getValues();
            }
        }
        return Collections.emptyList();
    }
    
    public class QueryFilterVariableResolver implements PrestoVariableResolver {

        private final PrestoVariableResolver variableResolver;
        private final String query;

        QueryFilterVariableResolver(PrestoContext context, String query) {
            this.query = query;
            this.variableResolver = new PrestoTopicFieldVariableResolver(context.getResolver());
        }

        @Override
        public List<? extends Object> getValues(Object value, String variable) {
            if (variable.equals(":query")) {
                return Collections.singletonList(query);
            } else {
                return variableResolver.getValues(value, variable);
            }
        }
        
    }

}
