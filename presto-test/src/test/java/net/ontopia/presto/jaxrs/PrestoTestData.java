package net.ontopia.presto.jaxrs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;
import net.ontopia.presto.spi.jackson.InMemoryJacksonDataProvider;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

public class PrestoTestData {

    public static void loadData(PrestoSchemaProvider schemaProvider, PrestoDataProvider dataProvider, String resource) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        InputStream istream = getInputStream(resource);
        if (istream != null) {
            try {
                JsonParser jp = mapper.getJsonFactory().createJsonParser(istream);
                jp.nextToken(); // ignore array start
                while (jp.nextToken() == JsonToken.START_OBJECT) {
                    ObjectNode doc = jp.readValueAs(ObjectNode.class);
                    if (dataProvider != null) {
                        createTopic(schemaProvider, dataProvider, doc);
                    }
                    System.out.println(doc);
                }
            } finally {
                istream.close();
            }
        } else {
            throw new RuntimeException("Could not find resource: " + resource);
        }
    }

    private static void createTopic(PrestoSchemaProvider schemaProvider, PrestoDataProvider dataProvider, ObjectNode doc) {
        String typeId = doc.path(":type").asText();
        if (typeId == null) {
            throw new RuntimeException("Topic ':type' is null: " + doc);
        }
        String topicId = doc.path("_id").asText();
        if (topicId == null) {
            throw new RuntimeException("Topic '_id' is null: " + doc);
        }
        PrestoType type = schemaProvider.getTypeById(typeId);
        PrestoChangeSet changeSet = dataProvider.newChangeSet();
        PrestoUpdate topic = changeSet.createTopic(type, topicId);
        Iterator<String> fieldIds = doc.getFieldNames();
        while (fieldIds.hasNext()) {
            String fieldId = fieldIds.next();
            
            char c = fieldId.charAt(0);
            if (c != '_' && c != ':') {
                PrestoField field = type.getFieldById(fieldId);
                Collection<Object> values = getFieldValues(dataProvider, doc, field);
                topic.setValues(field, values);
            }
        }
        changeSet.save();
    }

    private static Collection<Object> getFieldValues(PrestoDataProvider dataProvider, ObjectNode doc, PrestoField field) {
        String fieldId = field.getId();
        boolean isReferenceField = field.isReferenceField();
        Collection<Object> values = new ArrayList<Object>();
        JsonNode fieldNode = doc.path(fieldId);
        if (fieldNode.isArray()) {
            for (JsonNode fieldValueNode : fieldNode) {
                if (fieldValueNode.isTextual()) {
                    String valueId = fieldValueNode.asText();
                    if (isReferenceField) {
                        values.add(valueId);
                    } else {
                        values.add(valueId);
                    }
                }
            }
        } else if (fieldNode.isTextual()) {
            values.add(fieldNode.asText());
        }
        return values;
    }

    private static InputStream getInputStream(String resource) throws FileNotFoundException {
        return new FileInputStream("src/test/resources/net/ontopia/presto/jaxrs/" + resource);
//        return PrestoTestData.class.getClassLoader().getResourceAsStream(resource);
    }
    
    @Test
    public void testLoadingOfTestData() throws Exception {
        // load test data
        String databaseId = "test";
        PrestoDataProvider dataProvider = PrestoTestService.createDataProvider(databaseId);
        PrestoSchemaProvider schemaProvider = PrestoTestService.createSchemaProvider(databaseId);
        PrestoTestData.loadData(schemaProvider, dataProvider, "aass.json");

        // verify topic count
        InMemoryJacksonDataProvider dp = (InMemoryJacksonDataProvider)dataProvider;
        assertEquals("Number of topics", 3, dp.getSize());
        PrestoTopic aassBayer = dp.getTopicById("beer:aass-bayer");
        assertNotNull("Could not find topic 'beer:aass-bayer'", aassBayer);
        
        // foreign key checks
        PrestoTopic aass = dp.getTopicById("brewery:aass");
        PrestoType beerType = schemaProvider.getTypeById(aassBayer.getTypeId());
        PrestoField brewedBy = beerType.getFieldById("brewed-by");
        
        List<? extends Object> bayerBrewedBy = aassBayer.getValues(brewedBy);
        PrestoTopic hopefullyAass = (PrestoTopic)bayerBrewedBy.iterator().next();
        
        assertEquals("Incorrect brewed-by count", 1, bayerBrewedBy.size());
        assertTrue("Not brewed by brewery:aass", bayerBrewedBy.contains(aass));
        assertEquals("Names not equal", aass.getName(), hopefullyAass.getName());
    }

}
