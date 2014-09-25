package net.ontopia.presto.spi.utils;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoSchemaProvider;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface Handler {

    PrestoDataProvider getDataProvider();
    
    void setDataProvider(PrestoDataProvider dataProvider);

    PrestoSchemaProvider getSchemaProvider();

    void setSchemaProvider(PrestoSchemaProvider schemaProvider);

    ObjectNode getConfig();

    void setConfig(ObjectNode config);

}
