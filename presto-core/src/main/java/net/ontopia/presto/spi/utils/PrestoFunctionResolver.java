package net.ontopia.presto.spi.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.PrestoType;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class PrestoFunctionResolver implements PrestoFieldResolver {

    private final PrestoVariableContext context;
    private final ObjectNode config;

    public PrestoFunctionResolver(PrestoVariableContext context, ObjectNode config) {
        this.context = context;
        this.config = config;
    }

    @Override
    public PagedValues resolve(Collection<? extends Object> objects,
            PrestoType type, PrestoField field, boolean isReference, Paging paging) {
        
        PrestoFunction func = getFunction(config);
        if (func != null) {
            List<Object> result = func.execute(context, objects, type, field, paging);
            return new PrestoPagedValues(result, paging, result.size());            
        } else {
            return new PrestoPagedValues(Collections.emptyList(), paging, 0);        
        }
    }
    
    private PrestoFunction getFunction(ObjectNode resolveConfig) {
        JsonNode nameNode = resolveConfig.path("class");
        if (nameNode.isTextual()) {
            String className = nameNode.getTextValue();
            return Utils.newInstanceOf(className, PrestoFunction.class);
        }
        return null;
    }

}
