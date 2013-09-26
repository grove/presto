package net.ontopia.presto.jaxrs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;
import net.ontopia.presto.spi.utils.Utils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

public class PrestoTestData {

    public static void loadData(PrestoTestContext ctx, String resource) throws Exception {

        InputStream istream = getInputStream(resource);
        if (istream != null) {
            try {
                JsonParser jp = Utils.DEFAULT_OBJECT_MAPPER.getJsonFactory().createJsonParser(istream);
                jp.nextToken(); // ignore array start
                while (jp.nextToken() == JsonToken.START_OBJECT) {
                    ObjectNode doc = jp.readValueAs(ObjectNode.class);
                    createTopic(ctx, doc);
                    System.out.println(doc);
                }
            } finally {
                istream.close();
            }
        } else {
            throw new RuntimeException("Could not find resource: " + resource);
        }
    }

    private static void createTopic(PrestoTestContext ctx, ObjectNode doc) {
        PrestoSchemaProvider schemaProvider = ctx.getSchemaProvider();
        PrestoDataProvider dataProvider = ctx.getDataProvider();
        
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
        PrestoTestContext ctx = PrestoTestContext.create("test");
        PrestoTestData.loadData(ctx, "aass.json");
        
        // verify topic count
        ctx.assertTopicCount(3);
        
        PrestoTopic aassBayer = ctx.assertTopicExits("beer:aass-bayer");
        PrestoTopic aass = ctx.assertTopicExits("brewery:aass");
        
        // foreign key checks
        PrestoTopic hopefullAass = ctx.getFirstTopic(ctx.assertExactTopicValues(aassBayer, "brewed-by", Arrays.asList(aass)));
        
        // verify names of looked up topic and topic retrieved through field
        ctx.assertNamesEqual(aass, hopefullAass);
    }
}
