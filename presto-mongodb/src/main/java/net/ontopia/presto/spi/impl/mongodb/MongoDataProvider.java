package net.ontopia.presto.spi.impl.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.jackson.JacksonDataProvider;
import net.ontopia.presto.spi.jackson.JacksonTopic;
import net.vz.mongodb.jackson.DBCursor;
import net.vz.mongodb.jackson.JacksonDBCollection;
import net.vz.mongodb.jackson.WriteResult;

import org.bson.types.ObjectId;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.POJONode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

public abstract class MongoDataProvider extends JacksonDataProvider {

    private static Logger log = LoggerFactory.getLogger(MongoDataProvider.class.getName());

    protected Mongo mongo;
    protected DB db;
    protected DBCollection dbCollection;
    protected JacksonDBCollection<ObjectNode, Object> coll;
    
    public MongoDataProvider() {
        initialize();
    }

    protected void initialize() {
        try {
            mongo = new Mongo("127.0.0.1");
            db = mongo.getDB("presto");
            dbCollection = db.getCollection("demo");
            coll = JacksonDBCollection.wrap(dbCollection, ObjectNode.class);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    // -- PrestoDataProvider

    @Override
    public String getProviderId() {
        return "mongodb";
    }

    @Override
    public PrestoTopic getTopicById(String topicId) {
        // look up by document id
        ObjectNode doc = null;
        if (topicId != null) {
            doc = coll.findOne(new BasicDBObject("_id", internalTopicId(topicId)));
        }
        if (doc == null) {
            log.warn("Topic with id '" + topicId + "' not found.");
        }
        return existing(doc);
    }
    
    @Override
    public Collection<PrestoTopic> getTopicsByIds(Collection<String> topicIds) {        
        Collection<PrestoTopic> result = new ArrayList<PrestoTopic>();
        DBCursor<ObjectNode> cursor = coll.find(new BasicDBObject("_id", new BasicDBObject("$in", internalTopicIds(topicIds))));
        try {
            for (ObjectNode docNode : cursor) {
                if (docNode.isObject()) {
                    result.add(existing(docNode));
                }
            }
        } finally {
            cursor.close();
        }
        return result;
    }

    @Override
    public Collection<? extends Object> getAvailableFieldValues(PrestoTopic topic, final PrestoFieldUsage field) {
        if (field.isAddable()) {
            Collection<PrestoType> types = field.getAvailableFieldValueTypes();
            if (!types.isEmpty()) {
                
                BasicDBList typeIds = new BasicDBList();
                for (PrestoType type : types) {
                    typeIds.add(type.getId());
                }
    
                List<PrestoTopic> result = new ArrayList<PrestoTopic>(typeIds.size());
                
                DBCursor<ObjectNode> cursor = coll.find(new BasicDBObject(":type", new BasicDBObject("$in", typeIds)));
                try {
                    for (ObjectNode docNode : cursor) {
                        if (docNode.isObject()) {
                            result.add(existing(docNode));
                        }
                    }
                } finally {
                    cursor.close();
                }
                Collections.sort(result, new Comparator<PrestoTopic>() {
                    @Override
                    public int compare(PrestoTopic o1, PrestoTopic o2) {
                        return compareComparables(o1.getName(field), o2.getName(field));
                    }
                });
                return result;
            }
        }
        return Collections.emptyList();
    }

    @Override
    public void close() {
        mongo.close();
    }

    // -- id handling
    
    protected final static String OBJECT_ID_PREFIX = ":";
    
    protected Object internalTopicId(String topicId) {
        if (topicId.startsWith(OBJECT_ID_PREFIX)) {
            return new ObjectId(topicId.substring(1));
        } else {
            return topicId;
        }
    }

    protected Collection<Object> internalTopicIds(Collection<String> topicIds) {
        Collection<Object> result = new ArrayList<Object>();
        for (String topicId : topicIds) {
            result.add(internalTopicId(topicId));
        }
        return result;
    }
    
    protected String externalTopicId(JsonNode idNode) {
        if (idNode.isPojo()) { 
            Object pojo = ((POJONode)idNode).getPojo();
            return OBJECT_ID_PREFIX + pojo.toString();
        } else if (idNode.isTextual()) {
            return idNode.getTextValue();
        } else {
            throw new RuntimeException("Unknown id type: " + idNode);
        }    
    }

    // -- DefaultDataProvider

    @Override
    public void create(PrestoTopic topic) {
        ObjectNode data = ((JacksonTopic)topic).getData();
        WriteResult<ObjectNode, Object> result = coll.insert(data);
        ObjectNode saved = result.getSavedObject();
        JsonNode idNode = saved.get("_id");
        data.put("_id", idNode);
    }

    @Override
    public void update(PrestoTopic topic) {
        ObjectNode data = ((JacksonTopic)topic).getData();
        coll.updateById(internalTopicId(topic.getId()), data);
    }

    @Override
    public boolean delete(PrestoTopic topic) {
        coll.remove(new BasicDBObject("_id", internalTopicId(topic.getId())));
        return true;
    }

}
