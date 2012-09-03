package net.ontopia.presto.spi.impl.mongodb;

import java.util.Collection;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public interface IdentityStrategy {

    String generateId(String typeId, ObjectNode data);

    Object externalToInternalTopicId(String topicId);

    Collection<?> externalToInternalTopicIds(Collection<String> topicIds);
    
    String externalTopicId(JsonNode idNode);
    
}