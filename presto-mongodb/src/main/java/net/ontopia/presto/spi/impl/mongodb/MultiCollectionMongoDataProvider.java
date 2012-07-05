package net.ontopia.presto.spi.impl.mongodb;

import org.codehaus.jackson.node.ObjectNode;

import com.mongodb.Mongo;

public abstract class MultiCollectionMongoDataProvider extends MongoDataProvider {

    private static final String DATABASE_ID = "presto";

    @Override
    protected Mongo createMongo(String databaseId) {
        try {
            return new Mongo("127.0.0.1");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getCollectionKeyByTopicId(String topicId) {
        int cix = topicId.lastIndexOf(':');
        String typeId = topicId.substring(0, cix);
        return getCollectionKeyByTypeId(typeId);
    }

    @Override
    protected String getCollectionKeyByTypeId(String typeId) {
        return typeId;
    }
    
    @Override
    protected String getDatabaseIdByCollectionKey(String collectionKey) {
        return DATABASE_ID;
    }

    @Override
    protected String getCollectionIdByCollectionKey(String collectionKey) {
        return collectionKey;
    }

    @Override
    protected IdentityStrategy createIdentityStrategy() {
        return new UUIDIdentityStrategy() {
            @Override
            public String generateId(String typeId, ObjectNode data) {
                return typeId + ":" + super.generateId(typeId, data);
            }
        };
    }

}
