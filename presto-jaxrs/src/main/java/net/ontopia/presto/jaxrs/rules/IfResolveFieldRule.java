package net.ontopia.presto.jaxrs.rules;

import java.util.Collection;
import java.util.Collections;

import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.PrestoContextRules.FieldFlag;
import net.ontopia.presto.jaxrs.resolve.PrestoTopicWithParentFieldVariableResolver;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.jackson.JacksonDataProvider;
import net.ontopia.presto.spi.utils.PrestoPaging;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class IfResolveFieldRule extends BooleanFieldRule {

    @Override
    protected boolean getResult(FieldFlag flag, PrestoContext context, PrestoField field, ObjectNode config) {
        if (context.isNewTopic()) {
            return false;
        } else {
            if (config != null) {
                PrestoDataProvider dataProvider = getDataProvider();
    
                if (dataProvider instanceof JacksonDataProvider) {
    
                    Paging paging = new PrestoPaging(0, 1);
    
                    PrestoVariableResolver variableResolver = new PrestoTopicWithParentFieldVariableResolver(getSchemaProvider(), context);
    
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
    }

}
