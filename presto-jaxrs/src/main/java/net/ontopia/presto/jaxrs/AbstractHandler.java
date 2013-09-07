package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoSchemaProvider;

import org.codehaus.jackson.node.ObjectNode;

public class AbstractHandler implements Handler {

    private Presto presto;
    private ObjectNode config;
    
    @Override
    public Presto getPresto() {
        return presto;
    }

    @Override
    public void setPresto(Presto presto) {
        this.presto = presto;
    }

    @Override
    public PrestoDataProvider getDataProvider() {
        return presto.getDataProvider();
    }

    @Override
    public PrestoSchemaProvider getSchemaProvider() {
        return presto.getSchemaProvider();
    }

    @Override
    public ObjectNode getConfig() {
        return config;
    }

    @Override
    public void setConfig(ObjectNode config) {
        this.config = config;
    }

}
