package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoSchemaProvider;

import org.codehaus.jackson.node.ObjectNode;

public class AbstractHandler {

    private Presto presto;
    private ObjectNode config;
    
    protected Presto getPresto() {
        return presto;
    }

    public void setPresto(Presto presto) {
        this.presto = presto;
    }

    protected PrestoDataProvider getDataProvider() {
        return presto.getDataProvider();
    }

    protected PrestoSchemaProvider getSchemaProvider() {
        return presto.getSchemaProvider();
    }

    public ObjectNode getConfig() {
        return config;
    }

    public void setConfig(ObjectNode config) {
        this.config = config;
    }

}
