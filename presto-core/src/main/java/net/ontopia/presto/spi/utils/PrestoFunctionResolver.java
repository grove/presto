package net.ontopia.presto.spi.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoType;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrestoFunctionResolver implements PrestoFieldResolver {

    private static Logger log = LoggerFactory.getLogger(PrestoFunctionResolver.class.getName());

    private final PrestoContext context;

    public PrestoFunctionResolver(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, ObjectMapper objectMapper) {
        this.context = new PrestoContext(dataProvider, schemaProvider, objectMapper);
    }

    @Override
    public PagedValues resolve(Collection<? extends Object> objects,
            PrestoType type, PrestoField field, boolean isReference, ObjectNode resolveConfig, 
            boolean paging, int _limit, int offset, int limit) {
        
        PrestoFunction func = getFunction(context, resolveConfig);
        if (func != null) {
            List<Object> result = func.execute(context, objects, type, field, paging, _limit, offset, limit);
            return new PrestoPagedValues(result, offset, limit, result.size());            
        } else {
            return new PrestoPagedValues(Collections.emptyList(), 0, limit, 0);        
        }
    }
    
    private PrestoFunction getFunction(PrestoContext context, ObjectNode resolveConfig) {
        JsonNode nameNode = resolveConfig.path("class");
        if (nameNode.isTextual()) {
            String className = nameNode.getTextValue();
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            try {
                Class<?> klass = Class.forName(className, true, classLoader);
                if (PrestoFunction.class.isAssignableFrom(klass)) {
                    return (PrestoFunction) klass.newInstance();
                } else {
                    log.warn("Function class " + className + " not a PrestoFunction.");                    
                }
            } catch (ClassNotFoundException e) {
                log.warn("Function class " + className + " not found.");
            } catch (InstantiationException e) {
                log.warn("Not able to instatiate function class " + className + ".");
            } catch (IllegalAccessException e) {
                log.warn("Not able to instatiate function class " + className + " (illegal access).");
            }
        }
        return null;
    }

}
