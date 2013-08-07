package net.ontopia.presto.spi.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class PrestoTraverseResolver extends PrestoFieldResolver {

    @Override
    public PagedValues resolve(Collection<? extends Object> objects,
            PrestoField field, boolean isReference, Paging paging, PrestoVariableResolver variableResolver) {
        
        ObjectNode config = getConfig();
        
        if (config.has("path")) {
            JsonNode pathNode = config.get("path");
            if (pathNode.isArray()) {
                Collection<Object> rs = new HashSet<Object>(objects);
                for (JsonNode fieldItem : pathNode) {
                    // TODO: allow optional recursion
                    String fieldId = fieldItem.getTextValue();
                    rs = traverseField(rs, fieldId, variableResolver);
                }
                List<Object> result = new ArrayList<Object>(rs);
                return new PrestoPagedValues(result, paging, result.size());
            }
        }
        return new PrestoPagedValues(Collections.emptyList(), paging, 0);        
    }

    private Collection<Object> traverseField(Collection<Object> objects, String fieldId, PrestoVariableResolver variableResolver) {
        
        Collection<Object> result = new HashSet<Object>();
        if (objects.isEmpty()) {
            List<? extends Object> values = variableResolver.getValues(null, fieldId);
            result.addAll(values);
            
        } else {
            for (Object object : objects) {
                List<? extends Object> values = variableResolver.getValues(object, fieldId);
                result.addAll(values);
            }
        }
        return result;
    }

}
