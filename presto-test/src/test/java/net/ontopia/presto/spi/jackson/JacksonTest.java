package net.ontopia.presto.spi.jackson;

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;
import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.impl.pojo.PojoSchemaProvider;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

public class JacksonTest extends TestCase {

    private PojoSchemaProvider schemaProvider;
    private JacksonDataProvider dataProvider;

    protected void setUp() {
        this.schemaProvider = createSchemaProvider("test", "test.schema.json");
        this.dataProvider = createDataProvider();
    }

    static PojoSchemaProvider createSchemaProvider(String databaseId, String schemaFile) {
        return PojoSchemaProvider.getSchemaProvider(databaseId, schemaFile);
    }

    static InMemoryJacksonDataProvider createDataProvider() {
        return new InMemoryJacksonDataProvider() {
            @Override
            protected IdentityStrategy createIdentityStrategy() {
                return new UUIDIdentityStrategy();
            }
            @Override
            protected JacksonDataStrategy createDataStrategy(ObjectMapper mapper) {
                return new JacksonDefaultDataStrategy() {
                    @Override
                    public String getName(ObjectNode doc) {
                        return getSingleStringFieldValue(doc, "name");
                    }
                };
            }
        };
    }

    private void loadData(String filename) {
        DataLoader.loadData(dataProvider, schemaProvider, filename);
    }

    private ObjectMapper getObjectMapper() {
        return dataProvider.mapper;
    }
    
    private PrestoTopic getPerson() {
        ObjectNode data = getPersonObjectNode();
        return createTopic(data);
    }
    
    private ObjectNode getPersonObjectNode() {
        ObjectMapper mapper = getObjectMapper();
        ObjectNode data = mapper.createObjectNode();
        data.put("_id", "i:john.doe");
        data.put(":type", "c:person");
        data.put("name", createArray("John Doe"));
        data.put("age", createArray("26"));
        data.put("interests", createArray("sports", "beer"));
        return data;
    }
    
    private ArrayNode createArray(String... values) {
        ObjectMapper mapper = getObjectMapper();
        ArrayNode result = mapper.createArrayNode();
        for (String value : values) {
            result.add(value);
        }
        return result;
    }
    
    private PrestoTopic createTopic(ObjectNode data) {
        return new JacksonTopic(dataProvider, data);
    }
    
    @Test
    public void testGetId() {
        PrestoTopic topic = getPerson();
        assertEquals("i:john.doe", topic.getId());
    }
    
    @Test
    public void testGetType() {
        PrestoTopic topic = getPerson();
        assertEquals("c:person", topic.getTypeId());
    }
    
    @Test
    public void testGetName() {
        PrestoTopic topic = getPerson();
        assertEquals("John Doe", topic.getName());
    }
    
    @Test
    public void testGetAgeFieldValue() {
        PrestoTopic topic = getPerson();
        assertValuesEquals(Arrays.asList("26"), getFieldValues(topic, "age"));
    }
    
    @Test
    public void testGetInterestsFieldValue() {
        PrestoTopic topic = getPerson();
        assertValuesEquals(Arrays.asList("sports", "beer"), getFieldValues(topic, "interests"));
        
        loadData("test.data.json");
    }
    
    @Test
    public void testFavoriteBeer() {
        loadData("test.data.json");
        PrestoTopic johndoe = dataProvider.getTopicById("i:john.doe");
        PrestoTopic nogne_o_ipa = dataProvider.getTopicById("i:nogne-o-ipa");
        assertValuesEquals(Arrays.asList(nogne_o_ipa), getFieldValues(johndoe, "favorite-beer"));
    }
    
    @Test
    public void testInlineValues() {
        loadData("test.data.json");

        PrestoTopic johndoe = dataProvider.getTopicById("i:john.doe");
        List<? extends Object> fv = getFieldValues(johndoe, "hosted-drinking-sessions");
        assertEquals(2, fv.size());
        System.out.println(fv);

        PrestoTopic firstSession = (PrestoTopic)fv.get(0);
        assertTrue("First session is not an inline object", firstSession.isInline());
        assertEquals("1", firstSession.getId());
        
        List<? extends Object> attendeesFirstSession = getFieldValues(firstSession, "attendees");
        assertEquals(2, attendeesFirstSession.size());
        System.out.println(attendeesFirstSession);

        PrestoTopic firstAttendeeFirstSession = (PrestoTopic)attendeesFirstSession.get(0);
        assertFalse("First session attendee is an inline object", firstAttendeeFirstSession.isInline());
        assertEquals("i:john.travolta", firstAttendeeFirstSession.getId());
        assertEquals("John Travolta", firstAttendeeFirstSession.getName());
    }
 
    private List<? extends Object> getFieldValues(PrestoTopic topic, String fieldId) {
        String typeId = topic.getTypeId();
        PrestoType type = schemaProvider.getTypeById(typeId);
        PrestoField field = type.getFieldById(fieldId);
        return topic.getValues(field);
    }
    
    public static void assertValuesEquals(List<? extends Object> expected, List<? extends Object> actual) {
        if (expected.size() == actual.size()) {
            boolean equals = true;
            for (int i=0; i < actual.size(); i++) {
                if (!expected.get(i).equals(actual.get(i))) {
                    equals = false;
                    break;
                }
            }
            if (!equals) {
                failNotEquals("Values not equal", expected, actual);
            }
        } else {
            failNotEquals("Values not equal (different size)", expected, actual);
        }
    }
    
    @Test
    public void testCascadingDelete() {
        loadData("test.data.json");
        PrestoChangeSet changeSet = dataProvider.newChangeSet();

        PrestoType ratebeer_account = schemaProvider.getTypeById("c:ratebeer-account");
        PrestoTopic ratebeer_grove = dataProvider.getTopicById("i:ratebeer-grove");

        try {
            changeSet.deleteTopic(ratebeer_grove, ratebeer_account);
            changeSet.save();
            
            fail("Should not be allowed to delete topic because type is not removable.");
        } catch (Exception e) {
        }
    }
    
    @Test
    public void testCascadingDeleteThroughField() {
        loadData("test.data.json");
        PrestoChangeSet changeSet = dataProvider.newChangeSet();

        PrestoTopic ratebeer_grove = dataProvider.getTopicById("i:ratebeer-grove");
        assertNotNull("i:ratebeer-grove does not exist", ratebeer_grove);

        PrestoType person = schemaProvider.getTypeById("c:person");
        PrestoTopic grove = dataProvider.getTopicById("i:geir.ove.gronmo");
        assertNotNull("i:geir.ove.gronmo does not exist", grove);
        
        List<? extends Object> ratebeer_account = getFieldValues(grove, "ratebeer-account");
        assertEquals(1, ratebeer_account.size());
        changeSet.deleteTopic(grove, person); // NOTE: should also take i:ratebeer-grove with it
        changeSet.save();

        grove = dataProvider.getTopicById("i:geir.ove.gronmo");
        assertNull("i:geir.ove.gronmo not removed", grove);

        ratebeer_grove = dataProvider.getTopicById("i:ratebeer-grove");
        assertNull("i:ratebeer-grove not removed", ratebeer_grove);
    }

}