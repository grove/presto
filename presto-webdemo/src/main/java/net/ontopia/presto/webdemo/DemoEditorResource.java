package net.ontopia.presto.webdemo;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Path;

import net.ontopia.presto.jaxrs.EditorResource;
import net.ontopia.presto.spi.PrestoSession;
import net.ontopia.presto.spi.impl.couchdb.CouchDataProvider;
import net.ontopia.presto.spi.impl.pojo.PojoSchemaProvider;
import net.ontopia.presto.spi.impl.pojo.PojoSession;
import net.ontopia.presto.spi.impl.riak.RiakDataProvider;
import net.ontopia.presto.spi.jackson.JacksonBucketDataStrategy;
import net.ontopia.presto.spi.jackson.JacksonDataStrategy;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

@Path("/editor")
public class DemoEditorResource extends EditorResource {
	
    public static final String DB_NAME = "presto-demo";
    public static final String DESIGN_DOCUMENT = "_design/presto-demo";

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
    protected PrestoSession createSession(String databaseId) {

        PojoSchemaProvider schemaProvider = PojoSchemaProvider.getSchemaProvider(databaseId, databaseId + ".presto.json");
        
        // schema stored in json and data stored in couchdb
        final CouchDataProvider dataProvider = createCouchDbDataProvider();
//        final RiakDataProvider dataProvider = createRiakDataProvider();

        return new PojoSession(databaseId, getDatabaseName(databaseId), schemaProvider, dataProvider);
    }

    private CouchDataProvider createCouchDbDataProvider() {
        return new CouchDataProvider() {
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
                    protected List<String> getReadBuckets(ObjectNode doc) {
                        return READ_BUCKETS;
                    }
                    @Override
                    protected String getWriteBucket(ObjectNode doc) {
                        return WRITE_BUCKET;
                    }
                };
            }
        }.designDocId(DESIGN_DOCUMENT);
    }
    
    private RiakDataProvider createRiakDataProvider() {
        try {
            return new RiakDataProvider(DB_NAME) {
                @Override
                protected JacksonDataStrategy createDataStrategy(ObjectMapper mapper) {
                    return new JacksonBucketDataStrategy(mapper) {
                        @Override
                        protected List<String> getReadBuckets(ObjectNode doc) {
                            return READ_BUCKETS;
                        }
                        @Override
                        protected String getWriteBucket(ObjectNode doc) {
                            return WRITE_BUCKET;
                        }
                    };
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
