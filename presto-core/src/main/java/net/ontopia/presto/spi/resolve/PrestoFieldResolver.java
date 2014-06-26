package net.ontopia.presto.spi.resolve;

import java.util.Collection;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.utils.PrestoVariableContext;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

import org.codehaus.jackson.node.ObjectNode;

public abstract class PrestoFieldResolver {

    private PrestoVariableContext variableContext;
    private ObjectNode config;

    public PrestoDataProvider getDataProvider() {
        return getVariableContext().getDataProvider();
    }
    
    public PrestoVariableContext getVariableContext() {
        return variableContext;
    }
    public void setVariableContext(PrestoVariableContext variableContext) {
        this.variableContext = variableContext;
    }
    
    public ObjectNode getConfig() {
        return config;
    }
    public void setConfig(ObjectNode config) {
        this.config = config;
    }

    public abstract PagedValues resolve(Collection<? extends Object> objects,
            PrestoField field, boolean isReference, Projection projection, PrestoVariableResolver variableResolver);

}
