package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoSchemaProvider;

import org.junit.After;

public abstract class AbstractPrestoTest {
    
    protected PrestoSchemaProvider schemaProvider;
    protected PrestoDataProvider dataProvider;

    protected void createProviders(String databaseId) {
        this.schemaProvider = PrestoTestService.createSchemaProvider(databaseId);
        this.dataProvider = PrestoTestService.createDataProvider(databaseId, schemaProvider);
    }
    
    protected void createProviders(String databaseId, String filename) {
        createProviders(databaseId);
        DataLoader.loadData(dataProvider, schemaProvider, filename);
    }
        
    @After 
    public void tearDown() {
        this.dataProvider = null;
        this.schemaProvider = null;
    }

}
