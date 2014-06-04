package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoSchemaProvider;

import org.junit.After;

public abstract class AbstractPrestoTest {
    
    protected PrestoDataProvider dataProvider;
    protected PrestoSchemaProvider schemaProvider;

    protected void createProviders(String databaseId) {
        this.dataProvider = PrestoTestService.createDataProvider(databaseId);
        this.schemaProvider = PrestoTestService.createSchemaProvider(databaseId);
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
