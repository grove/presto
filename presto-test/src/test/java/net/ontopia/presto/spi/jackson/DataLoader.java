package net.ontopia.presto.spi.jackson;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public class DataLoader {
    
    private PrestoDataProvider dataProvider;
    private PrestoSchemaProvider schemaProvider;
    
    private Map<String,PrestoUpdate> topics = new HashMap<String,PrestoUpdate>();
    
    private DataLoader(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider) {
        this.dataProvider = dataProvider;
        this.schemaProvider = schemaProvider;
    }
    
    public static void loadData(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, String filename) {
        DataLoader dataLoader = new DataLoader(dataProvider, schemaProvider);
        dataLoader.loadData(filename);
    }
    
    private static final Set<String> IGNORE_FIELDS = new HashSet<String>() {{
        add("_id");
        add(":type");
        add(":name");
    }};

    public void loadData(String filename) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream istream = null;
        try {
            istream = cl.getResourceAsStream(filename);
            if (istream == null) {
                throw new RuntimeException("Cannot find schema file: " + filename);
            }
            Reader reader = new InputStreamReader(istream, "UTF-8");
            ObjectMapper mapper = new ObjectMapper();
            ArrayNode data = mapper.readValue(reader, ArrayNode.class);
            readData(data);

        } catch (Exception e) {
            throw new RuntimeException("Problems occured when loading '" + filename + "'", e);
        } finally {
            try {
                if (istream != null) istream.close();
            } catch (IOException e) {
            }
        }

    }

    private void readData(ArrayNode data) {
        PrestoChangeSet changeSet = dataProvider.newChangeSet();
        for (JsonNode d : data) {
            loadObjectNode(changeSet, d);
        }
        changeSet.save();
    }

    protected PrestoUpdate createTopic(PrestoChangeSet changeSet, PrestoType type, String topicId) {
        PrestoUpdate topic = topics.get(topicId);
        if (topic == null) {
            topic = changeSet.createTopic(type, topicId);
            topics.put(topicId, topic);
        }
        return topic;
    }
    
    public void loadObjectNode(PrestoChangeSet changeSet, JsonNode doc) {
        ObjectNode o = (ObjectNode)doc;
        String topicId = o.path("_id").asText();
        String typeId = o.path(":type").asText();

        PrestoType type = schemaProvider.getTypeById(typeId);
        PrestoUpdate topic = createTopic(changeSet, type, topicId);

        Iterator<String> fieldIds = o.getFieldNames();
        while (fieldIds.hasNext()) {
            String fieldId = fieldIds.next();
            if (!IGNORE_FIELDS.contains(fieldId)) {
                PrestoField field = type.getFieldById(fieldId);

                List<Object> values = new ArrayList<Object>();
                for (JsonNode value : o.get(fieldId)) {
//                    if (field.isReferenceField()) {
//                        String valueId = value.asText();                        
//                        PrestoTopic valueTopic = dataProvider.getTopicById(valueId);
//                        if (valueTopic != null) {
//                            values.add(valueTopic);
//                        } else {
//                            throw new RuntimeException("Could not find: "+ valueId);
//                        }
//                    } else {
                        values.add(value.asText());
//                    }
                }
                topic.addValues(field, values);
            }
        }
    }
}