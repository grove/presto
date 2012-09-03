package net.ontopia.presto.spi.impl.mongodb;

import java.util.UUID;

import org.codehaus.jackson.node.ObjectNode;

public class UUIDIdentityStrategy extends StringIdentityStrategy {
    @Override
    public String generateId(String typeId, ObjectNode data) {
        return UUID.randomUUID().toString();
    }
}