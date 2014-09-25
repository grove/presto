package net.ontopia.presto.spi.jackson;

import java.util.UUID;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class UUIDIdentityStrategy extends StringIdentityStrategy {
    @Override
    public String generateId(String typeId, ObjectNode data) {
        return UUID.randomUUID().toString();
    }
}