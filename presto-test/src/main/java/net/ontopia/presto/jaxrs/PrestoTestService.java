package net.ontopia.presto.jaxrs;

import java.util.Arrays;
import java.util.List;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.impl.couchdb.CouchDataProvider;
import net.ontopia.presto.spi.impl.mongodb.MongoDataProvider;
import net.ontopia.presto.spi.impl.mongodb.ObjectIdIdentityStrategy;
import net.ontopia.presto.spi.impl.pojo.PojoSchemaProvider;
import net.ontopia.presto.spi.impl.riak.RiakDataProvider;
import net.ontopia.presto.spi.jackson.DataProviderIdentityStrategy;
import net.ontopia.presto.spi.jackson.IdentityStrategy;
import net.ontopia.presto.spi.jackson.InMemoryJacksonDataProvider;
import net.ontopia.presto.spi.jackson.JacksonBucketDataStrategy;
import net.ontopia.presto.spi.jackson.JacksonDataStrategy;
import net.ontopia.presto.spi.jackson.UUIDIdentityStrategy;

import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class PrestoTestService {

    public static final String DB_NAME = "presto-test";
    public static final String COUCHDB_DESIGN_DOCUMENT = "_design/presto-test";

    public static final String BUCKET_WRITE = "write";
    public static final String BUCKET_READ = "read";
    public static final String BUCKET_INITIAL = "initial";

    public static final String WRITE_BUCKET = BUCKET_WRITE;
    
    public static final List<String> READ_BUCKETS = Arrays.asList(new String[] { BUCKET_WRITE, BUCKET_READ, BUCKET_INITIAL });

    public static PrestoSchemaProvider createSchemaProvider(String databaseId) {
        return PojoSchemaProvider.getSchemaProvider(databaseId, databaseId + ".schema.json");
    }

    public static PrestoDataProvider createDataProvider(String databaseId, PrestoSchemaProvider schemaProvider) {
        return createInMemoryDataProvider(schemaProvider);
//        return createCouchDbDataProvider(schemaProvider);
//        return createRiakDataProvider(schemaProvider);
//        return createMongoDataProvider(schemaProvider);
    }

    private static InMemoryJacksonDataProvider createInMemoryDataProvider(PrestoSchemaProvider schemaProvider) {
        return new InMemoryJacksonDataProvider(schemaProvider) {
            @Override
            protected JacksonDataStrategy createDataStrategy(ObjectMapper mapper) {
                return new JacksonBucketDataStrategy(mapper) {
                    @Override
                    public List<String> getReadBucketIds(ObjectNode doc) {
                        return READ_BUCKETS;
                    }
                    @Override
                    public String getWriteBucketId(ObjectNode doc) {
                        return WRITE_BUCKET;
                    }
                };
            }
            @Override
            protected IdentityStrategy createIdentityStrategy() {
                return new UUIDIdentityStrategy();
            }
        };
    }
    
    private static CouchDataProvider createCouchDbDataProvider(PrestoSchemaProvider schemaProvider) {
        return new CouchDataProvider(schemaProvider) {
            @Override
            protected CouchDbConnector createCouchDbConnector() {
                HttpClient httpClient = new StdHttpClient.Builder().build();
                CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
                return dbInstance.createConnector(DB_NAME, true);
            }

            @Override
            protected JacksonDataStrategy createDataStrategy(ObjectMapper mapper) {
                return new JacksonBucketDataStrategy(mapper) {
                    @Override
                    public List<String> getReadBucketIds(ObjectNode doc) {
                        return READ_BUCKETS;
                    }
                    @Override
                    public String getWriteBucketId(ObjectNode doc) {
                        return WRITE_BUCKET;
                    }
                };
            }

            @Override
            protected IdentityStrategy createIdentityStrategy() {
                return new DataProviderIdentityStrategy();
            }
        }.designDocId(COUCHDB_DESIGN_DOCUMENT);
    }
    
    private static RiakDataProvider createRiakDataProvider(PrestoSchemaProvider schemaProvider) {
        try {
            return new RiakDataProvider(schemaProvider, DB_NAME) {
                @Override
                protected JacksonDataStrategy createDataStrategy(ObjectMapper mapper) {
                    return new JacksonBucketDataStrategy(mapper) {
                        @Override
                        public List<String> getReadBucketIds(ObjectNode doc) {
                            return READ_BUCKETS;
                        }
                        @Override
                        public String getWriteBucketId(ObjectNode doc) {
                            return WRITE_BUCKET;
                        }
                    };
                }
                @Override
                protected IdentityStrategy createIdentityStrategy() {
                    return new DataProviderIdentityStrategy();
                }
            };
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private static MongoDataProvider createMongoDataProvider(PrestoSchemaProvider schemaProvider) {
        return new MongoDataProvider(schemaProvider) {
            
            private static final String COLLECTION_KEY = "test";
            
            private static final String DATABASE_ID = "presto";
            private static final String COLLECTION_ID = "test";
            
            @Override
            protected String getCollectionKeyByTopicId(String topicId) {
                return COLLECTION_KEY;
            }

            @Override
            protected String getCollectionKeyByTypeId(String typeId) {
                return COLLECTION_KEY;
            }

            @Override
            protected String getDatabaseIdByCollectionKey(String collectionKey) {
                return DATABASE_ID;
            }

            @Override
            protected String getCollectionIdByCollectionKey(String collectionKey) {
                return COLLECTION_ID;
            }
            
            @Override
            protected JacksonDataStrategy createDataStrategy(ObjectMapper mapper) {
                return new JacksonBucketDataStrategy(mapper) {
                    @Override
                    public List<String> getReadBucketIds(ObjectNode doc) {
                        return READ_BUCKETS;
                    }
                    @Override
                    public String getWriteBucketId(ObjectNode doc) {
                        return WRITE_BUCKET;
                    }
                };
            }

            @Override
            protected IdentityStrategy createIdentityStrategy() {
                return new ObjectIdIdentityStrategy();
            }

        };
    }
    
}
