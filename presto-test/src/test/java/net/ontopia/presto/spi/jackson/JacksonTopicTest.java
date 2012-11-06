package net.ontopia.presto.spi.jackson;

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.impl.pojo.PojoSchemaModel;
import net.ontopia.presto.spi.impl.pojo.PojoSchemaProvider;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

public class JacksonTopicTest extends TestCase {

    private JacksonDataProvider dataProvider;
    private PojoSchemaProvider schemaProvider;

    protected void setUp() {
        this.dataProvider = new InMemoryJacksonDataProvider() {
            @Override
            protected IdentityStrategy createIdentityStrategy() {
                return new UUIDIdentityStrategy();
            }
            @Override
            protected JacksonDataStrategy createDataStrategy(ObjectMapper mapper) {
                return new JacksonDefaultDataStrategy();
            }
        };
        this.schemaProvider = PojoSchemaModel.parse("test", "test.schema.json");
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
        data.put("_id", "johndoe");
        data.put(":type", "person");
        data.put(":name", "John Doe");
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
        assertEquals("johndoe", topic.getId());
    }
    
    @Test
    public void testGetType() {
        PrestoTopic topic = getPerson();
        assertEquals("person", topic.getTypeId());
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
        PrestoTopic johndoe = dataProvider.getTopicById("johndoe");
        PrestoTopic nogne_o_ipa = dataProvider.getTopicById("nogne-o-ipa");
        assertValuesEquals(Arrays.asList(nogne_o_ipa), getFieldValues(johndoe, "favorite-beer"));
    }
 
    private List<? extends Object> getFieldValues(PrestoTopic topic, String fieldId) {
        String typeId = topic.getTypeId();
        PrestoType type = schemaProvider.getTypeById(typeId);
        PrestoField field = type.getFieldById(fieldId);
        return topic.getValues(field);
    }
    
    private void assertValuesEquals(List<? extends Object> expected, List<? extends Object> actual) {
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
}
