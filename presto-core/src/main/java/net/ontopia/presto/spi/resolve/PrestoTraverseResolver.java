package net.ontopia.presto.spi.resolve;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.utils.PrestoPagedValues;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class PrestoTraverseResolver extends PrestoFieldResolver {

    @Override
    public PagedValues resolve(Collection<? extends Object> objects,
            PrestoField field, boolean isReference, Projection projection, 
            PrestoResolver prestoResolver, PrestoVariableResolver variableResolver) {
        
        ObjectNode config = getConfig();
        
        if (config.has("path")) {
            JsonNode pathNode = config.get("path");
            if (pathNode.isArray()) {
                Collection<Object> rs = new HashSet<Object>(objects);
                for (JsonNode fieldItem : pathNode) {
                    // TODO: allow optional recursion
                    String fieldId = fieldItem.textValue();
                    rs = traverseField(rs, fieldId, variableResolver);
                }
                List<Object> result = new ArrayList<Object>(rs);
                return new PrestoPagedValues(result, projection, result.size());
            }
        }
        return new PrestoPagedValues(Collections.emptyList(), projection, 0);        
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
