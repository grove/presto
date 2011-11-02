package net.ontopia.presto.spi.impl.pojo;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoSession;

public class PojoSession implements PrestoSession {

    private String databaseId;
    private String databaseName;

    private PrestoSchemaProvider schemaProvider;
    private PrestoDataProvider dataProvider;

    public PojoSession(String databaseId, String databaseName, PrestoSchemaProvider schemaProvider, PrestoDataProvider dataProvider) {
        this.databaseId = databaseId;
        this.databaseName = databaseName;
        this.schemaProvider = schemaProvider;
        this.dataProvider = dataProvider;        
    }

    @Override
    public String getDatabaseId() {
        return databaseId;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public void abort() {
    }

    @Override
    public void commit() {
    }

    @Override
    public void close() {
    }

    @Override
    public PrestoDataProvider getDataProvider() {
        return dataProvider;
    }

    @Override
    public PrestoSchemaProvider getSchemaProvider() {
        return schemaProvider;
    }

}
