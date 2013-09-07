package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoSchemaProvider;

import org.codehaus.jackson.node.ObjectNode;

public interface Handler {
    
    Presto getPresto();

    void setPresto(Presto presto);

    PrestoDataProvider getDataProvider();

    PrestoSchemaProvider getSchemaProvider();

    ObjectNode getConfig();

    void setConfig(ObjectNode config);

}
