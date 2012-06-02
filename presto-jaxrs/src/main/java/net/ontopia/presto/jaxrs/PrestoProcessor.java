package net.ontopia.presto.jaxrs;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

public abstract class PrestoProcessor {

    private PrestoDataProvider dataProvider;
    private PrestoSchemaProvider schemaProvider;

    public FieldData postProcess(FieldData fieldData, PrestoTopic topic, PrestoFieldUsage field) {
        return fieldData;
    }

    public Topic postProcess(Topic topic, PrestoType type) {
        return topic;
    }

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
