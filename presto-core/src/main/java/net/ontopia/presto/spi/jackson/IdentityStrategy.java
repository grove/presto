package net.ontopia.presto.spi.jackson;

import java.util.Collection;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface IdentityStrategy {

    /**
     * @param typeId
     * @param document
     * @return Return generated id. Null value means that we don't create one.
     */
    String generateId(String typeId, ObjectNode document);

    Object externalToInternalTopicId(String topicId);

    Collection<?> externalToInternalTopicIds(Collection<String> topicIds);
    
}