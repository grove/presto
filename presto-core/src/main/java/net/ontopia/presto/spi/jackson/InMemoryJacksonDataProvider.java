package net.ontopia.presto.spi.jackson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.node.ObjectNode;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;

public abstract class InMemoryJacksonDataProvider extends JacksonDataProvider {

    private Map<String,PrestoTopic> topics = new HashMap<String,PrestoTopic>();

    @Override
    public void create(PrestoTopic topic) {
        String topicId = topic.getId();
        String typeId = topic.getTypeId();
        if (topicId == null) {
            ObjectNode data = ((JacksonTopic)topic).getData();
            topicId = identityStrategy.generateId(typeId, data);
            data.put("_id", topicId);
        }
        
        if (topics.containsKey(topicId)) {
            throw new RuntimeException("Topic with id '" + topicId + "' already created.");
        } else {
            if (topicId == null) {
                throw new NullPointerException();
            }
            topics.put(topicId, topic);
        }
    }

    @Override
    public void update(PrestoTopic topic) {
        String topicId = topic.getId();
        if (topicId == null) {
            throw new NullPointerException();
        }
        if (topics.containsKey(topicId)) {
            topics.put(topicId, topic);
        } else {
            throw new RuntimeException("Topic with id '" + topicId + "' not an existing topic.");
        }
    }

    @Override
    public boolean delete(PrestoTopic topic) {
        String topicId = topic.getId();
        return topics.remove(topicId) != null;
    }

    @Override
    public String getProviderId() {
        return "in-memory-jackson";
    }

    @Override
    public PrestoTopic getTopicById(String topicId) {
        return topics.get(topicId);
    }

    @Override
    public Collection<PrestoTopic> getTopicsByIds(Collection<String> topicIds) {
        Collection<PrestoTopic> result = new ArrayList<PrestoTopic>();
        for (String topicId : topicIds) {
            PrestoTopic topic = getTopicById(topicId);
            if (topic != null) {
                result.add(topic);
            }
        }
        return result;
    }

    @Override
    public Collection<? extends Object> getAvailableFieldValues(PrestoTopic topic, PrestoField field, String query) {
        return Collections.emptyList();
    }

    @Override
    public void close() {
        topics.clear();
    }

//    @Override
//    protected JacksonDataStrategy createDataStrategy(ObjectMapper mapper) {
//        return null;
//    }

    public int getSize() {
        return topics.size();
    }
    
}
