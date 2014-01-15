package net.ontopia.presto.spi.resolve;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.utils.PrestoFunction;
import net.ontopia.presto.spi.utils.PrestoPagedValues;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;
import net.ontopia.presto.spi.utils.Utils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class PrestoFunctionResolver extends PrestoFieldResolver {

    @Override
    public PagedValues resolve(Collection<? extends Object> objects,
            PrestoField field, boolean isReference, Paging paging, PrestoVariableResolver variableResolver) {
        
        PrestoFunction func = getFunction(getConfig());
        if (func != null) {
            List<Object> result = func.execute(getVariableContext(), getConfig(), objects, field, paging);
            return new PrestoPagedValues(result, paging, result.size());            
        } else {
            return new PrestoPagedValues(Collections.emptyList(), paging, 0);        
        }
    }
    
    private PrestoFunction getFunction(ObjectNode resolveConfig) {
        JsonNode nameNode = resolveConfig.path("class");
        if (nameNode.isTextual()) {
            String className = nameNode.getTextValue();
            if (className != null) {
                return Utils.newInstanceOf(className, PrestoFunction.class);
            }
        }
        return null;
    }

}
