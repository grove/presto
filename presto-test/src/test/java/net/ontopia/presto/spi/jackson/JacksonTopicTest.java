package net.ontopia.presto.spi.jackson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoInlineTopicBuilder;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;
import net.ontopia.presto.spi.impl.pojo.PojoSchemaProvider;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JacksonTopicTest {

    private PojoSchemaProvider schemaProvider;
    private JacksonDataProvider dataProvider;

    @Before
    public void setUp() {
        this.schemaProvider = JacksonTest.createSchemaProvider("topic", "topic.schema.json");
        this.dataProvider = JacksonTest.createDataProvider();
    }

    private PrestoTopic createTopic(PrestoType type, String topicId) {
        PrestoTopic topic = dataProvider.getTopicById(topicId);
        if (topic != null) {
            return topic;
        }
        PrestoChangeSet changeSet = dataProvider.newChangeSet();
        PrestoUpdate update = changeSet.createTopic(type, topicId);
        changeSet.save();
        return update.getTopicAfterSave();
    }
    
    private PrestoTopic createInlineTopic(PrestoType type, String topicId) {
        PrestoInlineTopicBuilder builder = dataProvider.createInlineTopic(type, topicId);
        return builder.build();
    }
    
    private List<String> strings(String... strings) {
        return Arrays.asList(strings);
    }

    private List<PrestoTopic> topics(PrestoType type, String... strings) {
        List<PrestoTopic> result = new ArrayList<PrestoTopic>();
        for (String s : strings) {
            PrestoTopic topic = createTopic(type, s);
            result.add(topic);
        }
        return result;
    }

    private List<PrestoTopic> inlineTopics(PrestoType type, String... strings) {
        List<PrestoTopic> result = new ArrayList<PrestoTopic>();
        for (String s : strings) {
            PrestoTopic topic = createInlineTopic(type, s);
            result.add(topic);
        }
        return result;
    }

    private void isStringField(PrestoField field) {
        Assert.assertTrue("Field is a reference field", !field.isReferenceField());
        Assert.assertEquals("Field is not a string field", field.getDataType(), "http://www.w3.org/2001/XMLSchema#string");
        Assert.assertTrue("Field is an inline field", !field.isInline());
    }

    @Test
    public void testStringSetValues() {
        PrestoType type = schemaProvider.getTypeById("sometype");
        
        PrestoField field = type.getFieldById("strings");
        isStringField(field);
        
        JacksonTopic topic = (JacksonTopic)createTopic(type, "s0");
        
        // set A, B 
        topic.setValue(field, strings("A", "B"));        
        JacksonTest.assertValuesEquals(strings("A", "B"), topic.getValues(field));
        
        // set []
        topic.setValue(field, strings());        
        JacksonTest.assertValuesEquals(strings(), topic.getValues(field));
    }

    @Test
    public void testStringAddValues() {
        PrestoType type = schemaProvider.getTypeById("sometype");

        PrestoField field = type.getFieldById("strings");
        isStringField(field);
        
        JacksonTopic topic = (JacksonTopic)createTopic(type, "s1");
        
        // add A, B (at end)
        topic.addValue(field, strings("A", "B"), -1);        
        JacksonTest.assertValuesEquals(strings("A", "B"), topic.getValues(field));
        
        // add C, D, E (at end)
        topic.addValue(field, strings("C", "D", "E"), -1);        
        JacksonTest.assertValuesEquals(strings("A", "B", "C", "D", "E"), topic.getValues(field));
        
        // add F, G at index 0
        topic.addValue(field, strings("F", "G"), 0);        
        JacksonTest.assertValuesEquals(strings("F", "G", "A", "B", "C", "D", "E"), topic.getValues(field));

        // add H, I at index 4
        topic.addValue(field, strings("H", "I"), 4);        
        JacksonTest.assertValuesEquals(strings("F", "G", "A", "B", "H", "I", "C", "D", "E"), topic.getValues(field));
    }

    @Test
    public void testStringRemoveValues() {
        PrestoType type = schemaProvider.getTypeById("sometype");

        PrestoField field = type.getFieldById("strings");
        isStringField(field);
       
        JacksonTopic topic = (JacksonTopic)createTopic(type, "s2");
        
        // add A, B, C, D, E, F, G, H, I (at end)
        topic.addValue(field, strings("A", "B", "C", "D", "E", "F", "G", "H", "I"), -1);        
        JacksonTest.assertValuesEquals(strings("A", "B", "C", "D", "E", "F", "G", "H", "I"), topic.getValues(field));
        
        // remove A, B
        topic.removeValue(field, strings("A", "B"));        
        JacksonTest.assertValuesEquals(strings("C", "D", "E", "F", "G", "H", "I"), topic.getValues(field));
        
        // remove D, F
        topic.removeValue(field, strings("F", "D"));        
        JacksonTest.assertValuesEquals(strings("C", "E", "G", "H", "I"), topic.getValues(field));
        
        // remove C, I
        topic.removeValue(field, strings("C", "I"));        
        JacksonTest.assertValuesEquals(strings("E", "G", "H"), topic.getValues(field));
        
        // remove E, G, H
        topic.removeValue(field, strings("G", "E", "H"));        
        JacksonTest.assertValuesEquals(strings(), topic.getValues(field));
    }

    private void isReferenceField(PrestoField field) {
        Assert.assertTrue("Field is not a reference field", field.isReferenceField());
        Assert.assertEquals("Field is not a reference field", field.getDataType(), "reference");
        Assert.assertTrue("Field is not an inline field", !field.isInline());
    }

    @Test
    public void testTopicSetValues() {
        PrestoType type = schemaProvider.getTypeById("sometype");
        PrestoType vtype = schemaProvider.getTypeById("external-type");

        PrestoField field = type.getFieldById("topics");
        isReferenceField(field);
        
        JacksonTopic topic = (JacksonTopic)createTopic(type, "t0");
        
        // set A, B 
        topic.setValue(field, topics(vtype, "A", "B"));        
        JacksonTest.assertValuesEquals(topics(vtype, "A", "B"), topic.getValues(field));
        
        // set []
        topic.setValue(field, topics(vtype));        
        JacksonTest.assertValuesEquals(topics(vtype), topic.getValues(field));
    }

    @Test
    public void testTopicAddValues() {
        PrestoType type = schemaProvider.getTypeById("sometype");
        PrestoType vtype = schemaProvider.getTypeById("external-type");

        PrestoField field = type.getFieldById("topics");
        isReferenceField(field);
        
        JacksonTopic topic = (JacksonTopic)createTopic(type, "t1");
        
        // add A, B (at end)
        topic.addValue(field, topics(vtype, "A", "B"), -1);        
        JacksonTest.assertValuesEquals(topics(vtype, "A", "B"), topic.getValues(field));
        
        // add C, D, E (at end)
        topic.addValue(field, topics(vtype, "C", "D", "E"), -1);        
        JacksonTest.assertValuesEquals(topics(vtype, "A", "B", "C", "D", "E"), topic.getValues(field));
        
        // add F, G at index 0
        topic.addValue(field, topics(vtype, "F", "G"), 0);        
        JacksonTest.assertValuesEquals(topics(vtype, "F", "G", "A", "B", "C", "D", "E"), topic.getValues(field));

        // add H, I at index 4
        topic.addValue(field, topics(vtype, "H", "I"), 4);        
        JacksonTest.assertValuesEquals(topics(vtype, "F", "G", "A", "B", "H", "I", "C", "D", "E"), topic.getValues(field));
    }

    @Test
    public void testTopicRemoveValues() {
        PrestoType type = schemaProvider.getTypeById("sometype");
        PrestoType vtype = schemaProvider.getTypeById("external-type");

        PrestoField field = type.getFieldById("topics");
        isReferenceField(field);
        
        JacksonTopic topic = (JacksonTopic)createTopic(type, "t2");
        
        // add A, B, C, D, E, F, G, H, I (at end)
        topic.addValue(field, topics(vtype, "A", "B", "C", "D", "E", "F", "G", "H", "I"), -1);        
        JacksonTest.assertValuesEquals(topics(vtype, "A", "B", "C", "D", "E", "F", "G", "H", "I"), topic.getValues(field));
        
        // remove A, B
        topic.removeValue(field, topics(vtype, "A", "B"));        
        JacksonTest.assertValuesEquals(topics(vtype, "C", "D", "E", "F", "G", "H", "I"), topic.getValues(field));
        
        // remove D, F
        topic.removeValue(field, topics(vtype, "F", "D"));        
        JacksonTest.assertValuesEquals(topics(vtype, "C", "E", "G", "H", "I"), topic.getValues(field));
        
        // remove C, I
        topic.removeValue(field, topics(vtype, "C", "I"));        
        JacksonTest.assertValuesEquals(topics(vtype, "E", "G", "H"), topic.getValues(field));
        
        // remove E, G, H
        topic.removeValue(field, topics(vtype, "G", "E", "H"));        
        JacksonTest.assertValuesEquals(topics(vtype), topic.getValues(field));
    }

    private void isInlineReferenceField(PrestoField field) {
        Assert.assertTrue("Field is not a reference field", field.isReferenceField());
        Assert.assertEquals("Field is not a reference field", field.getDataType(), "reference");
        Assert.assertTrue("Field is not an inline field", field.isInline());
    }

    @Test
    public void testInlineTopicSetValues() {
        PrestoType type = schemaProvider.getTypeById("sometype");
        PrestoType vtype = schemaProvider.getTypeById("inline-type");

        PrestoField field = type.getFieldById("inline-topics");
        isInlineReferenceField(field);
        
        JacksonTopic topic = (JacksonTopic)createTopic(type, "it0");
        
        // set A, B 
        topic.setValue(field, inlineTopics(vtype, "A", "B"));        
        JacksonTest.assertValuesEquals(inlineTopics(vtype, "A", "B"), topic.getValues(field));
        
        // set []
        topic.setValue(field, inlineTopics(vtype));        
        JacksonTest.assertValuesEquals(inlineTopics(vtype), topic.getValues(field));
    }

    @Test
    public void testInlineTopicAddValues() {
        PrestoType type = schemaProvider.getTypeById("sometype");
        PrestoType vtype = schemaProvider.getTypeById("inline-type");

        PrestoField field = type.getFieldById("inline-topics");
        isInlineReferenceField(field);
        
        JacksonTopic topic = (JacksonTopic)createTopic(type, "it1");
        
        // add A, B (at end)
        topic.addValue(field, inlineTopics(vtype, "A", "B"), -1);        
        JacksonTest.assertValuesEquals(inlineTopics(vtype, "A", "B"), topic.getValues(field));
        
        // add C, D, E (at end)
        topic.addValue(field, inlineTopics(vtype, "C", "D", "E"), -1);        
        JacksonTest.assertValuesEquals(inlineTopics(vtype, "A", "B", "C", "D", "E"), topic.getValues(field));
        
        // add F, G at index 0
        topic.addValue(field, inlineTopics(vtype, "F", "G"), 0);        
        JacksonTest.assertValuesEquals(inlineTopics(vtype, "F", "G", "A", "B", "C", "D", "E"), topic.getValues(field));

        // add H, I at index 4
        topic.addValue(field, inlineTopics(vtype, "H", "I"), 4);        
        JacksonTest.assertValuesEquals(inlineTopics(vtype, "F", "G", "A", "B", "H", "I", "C", "D", "E"), topic.getValues(field));
    }

    @Test
    public void testInlineTopicRemoveValues() {
        PrestoType type = schemaProvider.getTypeById("sometype");
        PrestoType vtype = schemaProvider.getTypeById("inline-type");
        
        PrestoField field = type.getFieldById("inline-topics");
        isInlineReferenceField(field);
        
        JacksonTopic topic = (JacksonTopic)createTopic(type, "it2");
        
        // add A, B, C, D, E, F, G, H, I (at end)
        topic.addValue(field, inlineTopics(vtype, "A", "B", "C", "D", "E", "F", "G", "H", "I"), -1);        
        JacksonTest.assertValuesEquals(inlineTopics(vtype, "A", "B", "C", "D", "E", "F", "G", "H", "I"), topic.getValues(field));
        
        // remove A, B
        topic.removeValue(field, inlineTopics(vtype, "A", "B"));        
        JacksonTest.assertValuesEquals(inlineTopics(vtype, "C", "D", "E", "F", "G", "H", "I"), topic.getValues(field));
        
        // remove D, F
        topic.removeValue(field, inlineTopics(vtype, "F", "D"));        
        JacksonTest.assertValuesEquals(inlineTopics(vtype, "C", "E", "G", "H", "I"), topic.getValues(field));
        
        // remove C, I
        topic.removeValue(field, inlineTopics(vtype, "C", "I"));        
        JacksonTest.assertValuesEquals(inlineTopics(vtype, "E", "G", "H"), topic.getValues(field));
        
        // remove E, G, H
        topic.removeValue(field, inlineTopics(vtype, "G", "E", "H"));        
        JacksonTest.assertValuesEquals(inlineTopics(vtype), topic.getValues(field));
    }
    
}
