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
import net.ontopia.presto.spi.PrestoInlineTopicBuilder;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
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
        for (JsonNode doc : data) {
            loadTopic(changeSet, (ObjectNode)doc);
        }
        changeSet.save();
    }

    protected PrestoUpdate createUpdate(PrestoChangeSet changeSet, PrestoType type, String topicId) {
        PrestoUpdate update = topics.get(topicId);
        if (update == null) {
            update = changeSet.createTopic(type, topicId);
            topics.put(topicId, update);
        }
        return update;
    }
    
    private PrestoUpdate loadTopic(PrestoChangeSet changeSet, ObjectNode doc) {
        String topicId = doc.path("_id").asText();
        String typeId = doc.path(":type").asText();

        PrestoType type = schemaProvider.getTypeById(typeId);
        PrestoUpdate topic = createUpdate(changeSet, type, topicId);

        Iterator<String> fieldIds = doc.getFieldNames();
        while (fieldIds.hasNext()) {
            String fieldId = fieldIds.next();
            if (!IGNORE_FIELDS.contains(fieldId)) {
                PrestoField field = type.getFieldById(fieldId);
                List<Object> values = getFieldValues(changeSet, field, doc);
                topic.addValues(field, values);
            }
        }
        return topic;
    }

    private PrestoTopic loadInlineTopic(PrestoChangeSet changeSet, ObjectNode doc) {
        ObjectNode o = (ObjectNode)doc;
        String topicId = o.path("_id").asText();
        String typeId = o.path(":type").asText();

        PrestoType type = schemaProvider.getTypeById(typeId);

        PrestoInlineTopicBuilder builder = changeSet.createInlineTopic(type, topicId);

        Iterator<String> fieldIds = doc.getFieldNames();
        while (fieldIds.hasNext()) {
            String fieldId = fieldIds.next();
            if (!IGNORE_FIELDS.contains(fieldId)) {
                PrestoField field = type.getFieldById(fieldId);
                List<? extends Object> values = getFieldValues(changeSet, field, o);
                builder.setValues(field, values);
            }
        }
        return builder.build();
    }

    private List<Object> getFieldValues(PrestoChangeSet changeSet, PrestoField field, ObjectNode o) {
        List<Object> values = new ArrayList<Object>();
        for (JsonNode value : o.get(field.getId())) {
            if (field.isReferenceField()) {
                if (value.isObject()) {
                    PrestoTopic valueTopic = loadInlineTopic(changeSet, (ObjectNode)value);
                    values.add(valueTopic);
                } else if (value.isTextual()) {
                    if (value.isTextual()) {
                        values.add(value.asText());
                    } else {
                        throw new RuntimeException();
                    }
//                    String valueId = value.asText();
//                    PrestoTopicReference createReference = changeSet.createReference(null, valueId);
//                    PrestoTopic valueTopic = dataProvider.getTopicById(valueId);
//                    if (valueTopic != null) {
//                        values.add(valueTopic);
//                    } else {
//                        throw new RuntimeException("Could not find: "+ valueId);
//                    }
                } else {
                    throw new RuntimeException();
                }
            } else {
                if (value.isTextual()) {
                    values.add(value.asText());
                } else {
                    throw new RuntimeException();
                }
            }
        }
        return values;
    }
    
}