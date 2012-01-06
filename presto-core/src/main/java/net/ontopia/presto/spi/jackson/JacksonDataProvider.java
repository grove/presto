package net.ontopia.presto.spi.jackson;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.utils.PrestoFieldResolver;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.DefaultDataProvider;

public interface JacksonDataProvider extends DefaultDataProvider {

    ObjectMapper getObjectMapper();

    JacksonDataStrategy getDataStrategy();

    PrestoFieldResolver createFieldResolver(PrestoSchemaProvider schemaProvider, ObjectNode config);

}
