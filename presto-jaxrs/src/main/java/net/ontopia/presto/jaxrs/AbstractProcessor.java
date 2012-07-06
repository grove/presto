package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoSchemaProvider;

public class AbstractProcessor {

    private PrestoDataProvider dataProvider;
    private PrestoSchemaProvider schemaProvider;

    protected PrestoDataProvider getDataProvider() {
        return dataProvider;
    }
    
    public void setDataProvider(PrestoDataProvider dataProvider) {
        this.dataProvider = dataProvider;
    }

    protected PrestoSchemaProvider getSchemaProvider() {
        return schemaProvider;
    }
    
    public void setSchemaProvider(PrestoSchemaProvider schemaProvider) {
        this.schemaProvider = schemaProvider;
    }

}
