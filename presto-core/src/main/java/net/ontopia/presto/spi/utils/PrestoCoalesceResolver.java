package net.ontopia.presto.spi.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.jackson.JacksonDataProvider;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class PrestoCoalesceResolver extends PrestoFieldResolver {

    @Override
    public PagedValues resolve(Collection<? extends Object> objects,
            PrestoField field, boolean isReference, Paging paging, PrestoVariableResolver variableResolver) {

        ObjectNode config = getConfig();

        List<? extends Object> result = null;

        if (config != null && config.has("resolve")) {
            JsonNode resolveParentConfig = config.get("resolve");
            if (resolveParentConfig.isArray()) {
                for (JsonNode resolveConfig : resolveParentConfig) {
                    JacksonDataProvider jacksonDataProvider = (JacksonDataProvider)getDataProvider();
                    PagedValues values = jacksonDataProvider.resolveValues(objects, field, paging, resolveConfig, variableResolver);
                    result = values.getValues();
                    if (result != null) {
                        break;
                    }
                }
            }
        }
        if (result != null) {
            return new PrestoPagedValues(result, paging, 0);
        } else {
            return new PrestoPagedValues(Collections.emptyList(), paging, 0);
        }
    }
    
}
