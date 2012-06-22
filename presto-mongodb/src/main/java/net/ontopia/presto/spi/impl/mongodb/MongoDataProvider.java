package net.ontopia.presto.spi.impl.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.jackson.JacksonDataProvider;
import net.ontopia.presto.spi.jackson.JacksonTopic;
import net.vz.mongodb.jackson.JacksonDBCollection;
import net.vz.mongodb.jackson.WriteResult;

import org.bson.types.ObjectId;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            doc = coll.findOne(new BasicDBObject("_id", new ObjectId(topicId)));
        }
        if (doc == null) {
            log.warn("Topic with id '" + topicId + "' not found.");
        }
        return existing(doc);
    }

    @Override
    public Collection<PrestoTopic> getTopicsByIds(Collection<String> topicIds) {        
        Collection<PrestoTopic> result = new ArrayList<PrestoTopic>();
        for (String topicId : topicIds) {
            PrestoTopic topic = getTopicById(topicId);
            if (topic != null) {
                result.add(topic);
            }
        }
        return null;
    }

    @Override
    public Collection<? extends Object> getAvailableFieldValues(PrestoTopic topic, PrestoFieldUsage field) {
        return Collections.emptyList();
    }

    @Override
    public void close() {
        mongo.close();
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
        coll.updateById(topic.getId(), data);
    }

    @Override
    public boolean delete(PrestoTopic topic) {
        WriteResult<ObjectNode, Object> result = coll.remove(new BasicDBObject("_id", new ObjectId(topic.getId())));
        return true; // TODO: what to do?
    }

}
