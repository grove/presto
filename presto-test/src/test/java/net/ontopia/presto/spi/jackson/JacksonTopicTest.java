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
        schemaProvider = PojoSchemaModel.parse("test", "test.presto.json");
    }

    private PrestoTopic createSimpleTopic() {
        ObjectMapper mapper = dataProvider.mapper;
        ObjectNode data = mapper.createObjectNode();
        data.put("_id", "johndoe");
        data.put(":type", "person");
        data.put(":name", "John Doe");
        data.put("age", createArray("26"));
        return createTopic(data);
    }
    
    private ArrayNode createArray(String... values) {
        ObjectMapper mapper = dataProvider.mapper;
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
        PrestoTopic topic = createSimpleTopic();
        assertEquals("johndoe", topic.getId());
    }
    
    @Test
    public void testGetType() {
        PrestoTopic topic = createSimpleTopic();
        assertEquals("person", topic.getTypeId());
    }
    
    @Test
    public void testGetName() {
        PrestoTopic topic = createSimpleTopic();
        assertEquals("John Doe", topic.getName());
    }
    
    @Test
    public void testGetAgeFieldValue() {
        PrestoTopic topic = createSimpleTopic();
        PrestoType type = schemaProvider.getTypeById("person");
        PrestoField field = type.getFieldById("age");
        assertValuesEquals(Arrays.asList("26"), topic.getValues(field));
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
