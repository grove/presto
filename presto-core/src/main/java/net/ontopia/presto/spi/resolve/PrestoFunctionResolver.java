package net.ontopia.presto.spi.resolve;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.utils.PrestoPagedValues;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;
import net.ontopia.presto.spi.utils.Utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class PrestoFunctionResolver extends PrestoFieldResolver {

    @Override
    public PagedValues resolve(Collection<? extends Object> objects,
            PrestoField field, boolean isReference, Projection projection, PrestoVariableResolver variableResolver) {
        
        PrestoResolverFunction func = getFunction(getConfig());
        if (func != null) {
            List<Object> result = func.execute(getVariableContext(), getConfig(), objects, field, projection);
            return new PrestoPagedValues(result, projection, result.size());            
        } else {
            return new PrestoPagedValues(Collections.emptyList(), projection, 0);        
        }
    }
    
    private PrestoResolverFunction getFunction(ObjectNode resolveConfig) {
        JsonNode nameNode = resolveConfig.path("class");
        if (nameNode.isTextual()) {
            String className = nameNode.textValue();
            if (className != null) {
                return Utils.newInstanceOf(className, PrestoResolverFunction.class);
            }
        }
        return null;
    }

}
