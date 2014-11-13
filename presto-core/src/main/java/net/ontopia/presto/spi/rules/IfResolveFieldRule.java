package net.ontopia.presto.spi.rules;

import java.util.Collection;
import java.util.Collections;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.jackson.JacksonDataProvider;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldFlag;
import net.ontopia.presto.spi.utils.PrestoProjection;
import net.ontopia.presto.spi.utils.PrestoTopicWithParentFieldVariableResolver;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class IfResolveFieldRule extends BooleanFieldRule {

    @Override
    protected boolean getResult(FieldFlag flag, PrestoContextRules rules, PrestoField field, ObjectNode config) {
        PrestoContext context = rules.getContext();
        if (context.isNewTopic()) {
            return false;
        } else {
            return ifResolve(context, field, config);
        }
    }

    private boolean ifResolve(PrestoContext context, PrestoField field, ObjectNode config) {
        if (config != null) {
            PrestoDataProvider dataProvider = getDataProvider();
   
            if (dataProvider instanceof JacksonDataProvider) {
   
                Projection projection = PrestoProjection.FIRST_ELEMENT;
   
                PrestoTopic topic = context.getTopic();
   
                Collection<? extends Object> objects = (topic == null ? Collections.emptyList() : Collections.singleton(topic));
   
                JsonNode resolveConfig = config.path("resolve");
                
                PrestoVariableResolver variableResolver = new PrestoTopicWithParentFieldVariableResolver(context);

                PagedValues values = context.resolveValues(objects, field, projection, resolveConfig, variableResolver);
   
                return !values.getValues().isEmpty();
            }
        }
        return false;
    }

}
