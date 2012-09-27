package net.ontopia.presto.spi.jackson;

import org.codehaus.jackson.node.ObjectNode;

public class DataProviderIdentityStrategy extends StringIdentityStrategy {

    @Override
    public String generateId(String typeId, ObjectNode document) {
        return null;
    }

}
