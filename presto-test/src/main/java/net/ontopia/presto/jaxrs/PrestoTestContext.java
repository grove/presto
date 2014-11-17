package net.ontopia.presto.jaxrs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.jackson.InMemoryJacksonDataProvider;

public class PrestoTestContext {

    private final PrestoSchemaProvider schemaProvider;
    private final PrestoDataProvider dataProvider;

    PrestoTestContext(String contextId) {
        this.schemaProvider = PrestoTestService.createSchemaProvider(contextId);
        this.dataProvider = PrestoTestService.createDataProvider(contextId, schemaProvider);
    }

    public static PrestoTestContext create(String databaseId) {
        return new PrestoTestContext(databaseId);
    }

    public PrestoDataProvider getDataProvider() {
        return dataProvider;
    }

    public PrestoSchemaProvider getSchemaProvider() {
        return schemaProvider;
    }

    public int getTopicCount() {
        InMemoryJacksonDataProvider dp = (InMemoryJacksonDataProvider)dataProvider;
        return dp.getSize();
    }

    public PrestoTopic getTopicById(String topicId) {
        return dataProvider.getTopicById(topicId);
    }

    public PrestoField getFieldById(PrestoTopic topic, String fieldId) {
        PrestoType type = schemaProvider.getTypeById(topic.getTypeId());
        return type.getFieldById(fieldId);
    }

    public PrestoTopic getFirstTopicValue(PrestoTopic topic, String fieldId) {
        return getFirstTopic(getValues(topic, fieldId));
    }
    
    public PrestoTopic getFirstTopic(List<? extends Object> values) {
        if (values.isEmpty()) {
            return null;
        } else {
            return (PrestoTopic)values.iterator().next();
        }
    }

    public List<? extends Object> getValues(PrestoTopic topic, String fieldId) {
        PrestoField field = getFieldById(topic, fieldId);
        return topic.getValues(field);
    }

    // -- assert methods
    
    public int assertTopicCount(int i) {
        int count = getTopicCount();
        assertEquals("Number of topics", i, count);
        return count;
    }

    public PrestoTopic assertTopicExits(String topicId) {
        PrestoTopic topic = getTopicById(topicId);
        assertNotNull("Could not find topic '" + topicId + "'", topic);
        return topic;
    }

    public List<? extends Object> assertExactTopicIdValues(PrestoTopic topic, String fieldId, List<String> topicIds) {
        List<PrestoTopic> topics = new ArrayList<PrestoTopic>();
        for (String topicId : topicIds) {
            topics.add(assertTopicExits(topicId));
        }
        return assertExactTopicValues(topic, fieldId, topics);
    }
    
    public List<? extends Object> assertExactTopicValues(PrestoTopic topic, String fieldId, List<? extends PrestoTopic> topics) {
        List<? extends Object> result = getValues(topic, fieldId);
        assertEquals("Incorrect collection size", topics.size(), result.size());
        assertTrue("Collection does not contain all elements", result.containsAll(topics));
        return result;
    }

    public void assertNamesEqual(PrestoTopic topic1, PrestoTopic topic2) {
        assertEquals("Names not equal", topic1.getName(), topic2.getName());
    }
    
}
