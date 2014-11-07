package net.ontopia.presto.spi.jackson;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class DataProviderIdentityStrategy extends StringIdentityStrategy {

    @Override
    public String generateId(String typeId, ObjectNode document) {
        return null;
    }

}
