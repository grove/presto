package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoSchemaProvider;

public class AbstractProcessor {

    private Presto presto;

    public void setPresto(Presto presto) {
        this.presto = presto;
    }

    protected Presto getPresto() {
        return presto;
    }
    
    protected PrestoDataProvider getDataProvider() {
        return presto.getDataProvider();
    }

    protected PrestoSchemaProvider getSchemaProvider() {
        return presto.getSchemaProvider();
    }

}
