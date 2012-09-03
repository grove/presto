package net.ontopia.presto.spi.impl.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.POJONode;

public class ObjectIdIdentityStrategy implements IdentityStrategy {
    
    protected final static String OBJECT_ID_PREFIX = ":";
    
    @Override
    public String generateId(String typeId, ObjectNode data) {
        return UUID.randomUUID().toString();
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
    
    @Override
    public String externalTopicId(JsonNode idNode) {
        if (idNode.isPojo()) { 
            Object pojo = ((POJONode)idNode).getPojo();
            return OBJECT_ID_PREFIX + pojo.toString();
        } else if (idNode.isTextual()) {
            return idNode.getTextValue();
        } else {
            throw new RuntimeException("Unknown id type: " + idNode);
        }    
    }
}