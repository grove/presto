package net.ontopia.presto.webdemo;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Path;

import net.ontopia.presto.jaxrs.EditorResource;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.impl.couchdb.CouchDataProvider;
import net.ontopia.presto.spi.impl.pojo.PojoSchemaProvider;
import net.ontopia.presto.spi.impl.riak.RiakDataProvider;
import net.ontopia.presto.spi.jackson.DataProviderIdentityStrategy;
import net.ontopia.presto.spi.jackson.IdentityStrategy;
import net.ontopia.presto.spi.jackson.JacksonBucketDataStrategy;
import net.ontopia.presto.spi.jackson.JacksonDataStrategy;

import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Path("/editor")
public class DemoEditorResource extends EditorResource {
	
    public static final String DB_NAME = "presto-demo";
    public static final String COUCHDB_DESIGN_DOCUMENT = "_design/presto-demo";

    public static final String BUCKET_WRITE = "write";
    public static final String BUCKET_READ = "read";
    public static final String BUCKET_INITIAL = "initial";

    public static final String WRITE_BUCKET = BUCKET_WRITE;
	
    public static final List<String> READ_BUCKETS = Arrays.asList(new String[] { BUCKET_WRITE, BUCKET_READ, BUCKET_INITIAL });
	
    private Map<String,String> databases = new HashMap<String,String>();
	
    public DemoEditorResource() {
        databases.put("beer", "Beer database");
    }

    @Override
    protected Presto createPresto(String databaseId, final boolean readOnlyMode) {

        // schema stored in json format
        PojoSchemaProvider schemaProvider = PojoSchemaProvider.getSchemaProvider(databaseId, databaseId + ".presto.json");
        
        // data stored in couchdb
        final CouchDataProvider dataProvider = createCouchDbDataProvider(schemaProvider);
        
        // data stored in riak
//        final RiakDataProvider dataProvider = createRiakDataProvider(schemaProvider);

        return new EditorResourcePresto(databaseId, getDatabaseName(databaseId), schemaProvider, dataProvider, getAttributes()) {
            @Override
            public boolean isReadOnlyMode() {
                return readOnlyMode;
            }
        };
    }

    private CouchDataProvider createCouchDbDataProvider(PrestoSchemaProvider schemaProvider) {
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
    
    @SuppressWarnings("unused")
    private RiakDataProvider createRiakDataProvider(PrestoSchemaProvider schemaProvider) {
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
    
    @Override
    protected Collection<String> getDatabaseIds() {          
        return databases.keySet();
    }

    @Override 
    protected String getDatabaseName(String databaseId) {
        if (databaseId != null && databases.containsKey(databaseId)) {
            return databases.get(databaseId);
        }
        throw new RuntimeException("Unknown database: " + databaseId);
    }

}
