package net.ontopia.presto.spi.impl.mongodb;

import java.util.ArrayList;
import java.util.Collection;

import net.ontopia.presto.spi.jackson.IdentityStrategy;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class ObjectIdIdentityStrategy implements IdentityStrategy {
    
    protected final static String OBJECT_ID_PREFIX = ":";
    
    @Override
    public String generateId(String typeId, ObjectNode data) {
        return null;
    }
    
    @Override
    public Object externalToInternalTopicId(String topicId) {
        if (topicId.startsWith(OBJECT_ID_PREFIX)) {
            return new ObjectId(topicId.substring(1));
        } else {
            return topicId;
        }
    }

    @Override
    public Collection<Object> externalToInternalTopicIds(Collection<String> topicIds) {
        Collection<Object> result = new ArrayList<Object>();
        for (String topicId : topicIds) {
            result.add(externalToInternalTopicId(topicId));
        }
        return result;
    }

}