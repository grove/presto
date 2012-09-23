package net.ontopia.presto.spi.jackson;

import java.util.Collection;

public abstract class StringIdentityStrategy implements IdentityStrategy {
    
    @Override
    public Object externalToInternalTopicId(String topicId) {
        return topicId;
    }

    @Override
    public Collection<?> externalToInternalTopicIds(Collection<String> topicIds) {
        return topicIds;
    }

}