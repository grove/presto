package net.ontopia.presto.spi.impl.mongodb;

import java.util.Collection;

import org.codehaus.jackson.JsonNode;

public abstract class StringIdentityStrategy implements IdentityStrategy {
    
    @Override
    public Object externalToInternalTopicId(String topicId) {
        return topicId;
    }

    @Override
    public Collection<?> externalToInternalTopicIds(Collection<String> topicIds) {
        return topicIds;
    }
    
    @Override
    public String externalTopicId(JsonNode idNode) {
        if (idNode.isTextual()) {
            return idNode.getTextValue();
        } else {
            throw new RuntimeException("Unknown id type: " + idNode);
        }    
    }
}