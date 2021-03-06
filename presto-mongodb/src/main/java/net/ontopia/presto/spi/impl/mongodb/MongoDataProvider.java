package net.ontopia.presto.spi.impl.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.jackson.JacksonDataProvider;
import net.ontopia.presto.spi.jackson.JacksonTopic;
import net.ontopia.presto.spi.utils.Utils;

import org.mongojack.DBCursor;
import org.mongojack.JacksonDBCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;

public abstract class MongoDataProvider extends JacksonDataProvider {

    private static Logger log = LoggerFactory.getLogger(MongoDataProvider.class);

    private Map<String,Mongo> mongos = new HashMap<String,Mongo>();
    private Map<String,JacksonDBCollection<ObjectNode, Object>> collections = new HashMap<String,JacksonDBCollection<ObjectNode, Object>>();
    
    public MongoDataProvider(PrestoSchemaProvider schemaProvider) {
        super(schemaProvider);
    }

    private static final String DEFAULT_MONGO_URI = "mongodb://localhost";

    protected Mongo createMongo(String databaseId) {
        try {
            return new Mongo(new MongoURI(getMongoURI()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected String getMongoURI() {
        return DEFAULT_MONGO_URI;
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
            doc = findTopicById(topicId);
        }
        if (doc == null) {
            PrestoTopic topic = lazyLoad(topicId);
            if (topic != null) {
                return topic;
            }
            log.warn("Topic with id '" + topicId + "' not found.");
        }
        return existing(doc);
    }
    
    @Override
    public Collection<PrestoTopic> getTopicsByIds(Collection<String> topicIds) {        
        Collection<PrestoTopic> result = new ArrayList<PrestoTopic>();
        aggregateTopicsById(topicIds, result);        
        return includeLazyTopics(result, topicIds);
    }

    @Override
    public Collection<? extends Object> getAvailableFieldValues(PrestoTopic topic, final PrestoField field, String query) {
        Collection<PrestoType> types = field.getAvailableFieldValueTypes();
        if (types.isEmpty()) {
            return Collections.emptyList();
        }
        List<PrestoTopic> result = new ArrayList<PrestoTopic>();
        aggregateTopicsByType(types, result);

        Collections.sort(result, new Comparator<PrestoTopic>() {
            @Override
            public int compare(PrestoTopic o1, PrestoTopic o2) {
                return Utils.compareComparables(o1.getName(field), o2.getName(field));
            }
        });
        return result;
    }

    @Override
    public void close() {
        for (Mongo mongo : mongos.values()) {
            try {
                mongo.close();
            } catch (Exception e) {
                log.warn("Could not close mongo connection: " + mongo, e);
            }
        }
    }

    // -- DefaultDataProvider

    @Override
    public void create(PrestoTopic topic) {
        ObjectNode data = ((JacksonTopic)topic).getData();
        String typeId = topic.getTypeId();
        JacksonDBCollection<ObjectNode, Object> collection = getCollectionByTypeId(typeId);
        String topicId = identityStrategy.generateId(typeId, data);
        if (topicId != null) {
            data.put("_id", topicId);
        }
        collection.insert(data);
    }
    
    @Override
    public void update(PrestoTopic topic) {
        ObjectNode data = ((JacksonTopic)topic).getData();
        JacksonDBCollection<ObjectNode, Object> collection = getCollectionByTypeId(topic.getTypeId());
        collection.updateById(identityStrategy.externalToInternalTopicId(topic.getId()), data);
    }

    @Override
    public boolean delete(PrestoTopic topic) {
        JacksonDBCollection<ObjectNode, Object> collection = getCollectionByTypeId(topic.getTypeId());
        collection.remove(new BasicDBObject("_id", identityStrategy.externalToInternalTopicId(topic.getId())));
        return true;
    }

    // -- data collections strategy

    protected JacksonDBCollection<ObjectNode, Object> getCollectionByKey(String collectionKey) {
        JacksonDBCollection<ObjectNode, Object> coll = collections.get(collectionKey);
        if (coll == null) {
            String databaseId = getDatabaseIdByCollectionKey(collectionKey);
            Mongo mongo = getMongo(databaseId);
            DB db = mongo.getDB(databaseId);
            String collectionId = getCollectionIdByCollectionKey(collectionKey);
            DBCollection dbCollection = db.getCollection(collectionId);
            coll = JacksonDBCollection.wrap(dbCollection, ObjectNode.class);
            collections.put(collectionId, coll);
        }
        return coll;
    }
    
    protected Mongo getMongo(String databaseId) {
        Mongo mongo = mongos.get(databaseId);
        if (mongo == null) {
            mongo = createMongo(databaseId);
            mongos.put(databaseId, mongo);
        }
        return mongo;
    }
    
    protected JacksonDBCollection<ObjectNode, Object> getCollectionByTopicId(String topicId) {
        String collectionKey = getCollectionKeyByTopicId(topicId);
        return getCollectionByKey(collectionKey);
    }
    
    protected JacksonDBCollection<ObjectNode, Object> getCollectionByTypeId(String typeId) {
        String collectionKey = getCollectionKeyByTypeId(typeId);
        return getCollectionByKey(collectionKey);
    }

    protected abstract String getCollectionKeyByTopicId(String topicId);

    protected abstract String getCollectionKeyByTypeId(String typeId);
    
    protected abstract String getDatabaseIdByCollectionKey(String collectionKey);

    protected abstract String getCollectionIdByCollectionKey(String collectionKey);

    protected ObjectNode findTopicById(String topicId) {
        JacksonDBCollection<ObjectNode, Object> coll = getCollectionByTopicId(topicId);
        return coll.findOne(new BasicDBObject("_id", getIdentityStrategy().externalToInternalTopicId(topicId)));
    }
    
    protected void aggregateTopicsById(Collection<String> topicIds, Collection<PrestoTopic> result) {
        Map<String,List<String>> collectionKeys = new HashMap<String,List<String>>();
        for (String topicId : topicIds) {
            String collectionKey = getCollectionKeyByTopicId(topicId);
            List<String> partitionedTopicIds = collectionKeys.get(collectionKey);
            if (partitionedTopicIds == null) {
                partitionedTopicIds = new ArrayList<String>();
                collectionKeys.put(collectionKey, partitionedTopicIds);
            }
            partitionedTopicIds.add(topicId);
        }
        for (String collectionKey : collectionKeys.keySet()) {
            JacksonDBCollection<ObjectNode, Object> coll = getCollectionByKey(collectionKey);
            List<String> partitionedTopicIds = collectionKeys.get(collectionKey);
            aggregateResult(coll.find(new BasicDBObject("_id", new BasicDBObject("$in", getIdentityStrategy().externalToInternalTopicIds(partitionedTopicIds)))), result);
        }
    }
    
    protected void aggregateTopicsByType(Collection<PrestoType> types, Collection<PrestoTopic> result) {
        Map<String,BasicDBList> collectionKeys = new HashMap<String,BasicDBList>();
        for (PrestoType type : types) {
            String typeId = type.getId();
            String collectionKey = getCollectionKeyByTypeId(typeId);
            BasicDBList partitionedTypeIds = collectionKeys.get(collectionKey);
            if (partitionedTypeIds == null) {
                partitionedTypeIds = new BasicDBList();
                collectionKeys.put(collectionKey, partitionedTypeIds);
            }
            partitionedTypeIds.add(typeId);
        }
        for (String collectionKey : collectionKeys.keySet()) {
            JacksonDBCollection<ObjectNode, Object> coll = getCollectionByKey(collectionKey);
            BasicDBList partitionedTypeIds = collectionKeys.get(collectionKey);
            aggregateResult(coll.find(new BasicDBObject(":type", new BasicDBObject("$in", partitionedTypeIds))), result);
        }
    }
    
    protected void aggregateResult(DBCursor<ObjectNode> cursor, Collection<PrestoTopic> result) {
        try {
            for (ObjectNode docNode : cursor) {
                if (docNode.isObject()) {
                    result.add(existing(docNode));
                }
            }
        } finally {
            cursor.close();
        }        
    }

}
