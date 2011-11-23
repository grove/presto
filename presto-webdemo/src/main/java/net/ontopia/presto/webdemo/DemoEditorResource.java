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
import net.ontopia.presto.spi.jackson.JacksonBucketFieldStrategy;
import net.ontopia.presto.spi.jackson.JacksonFieldStrategy;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

@Path("/editor")
public class DemoEditorResource extends EditorResource {
	
    public static final String DB_NAME = "pesto";
    public static final String DESIGN_DOCUMENT = "_design/pesto";

    public static final String BUCKET_PESTO = "pesto";
    public static final String BUCKET_BOKDB = "bokdb";
    public static final String BUCKET_INITIAL = "initial";

    public static final String WRITE_BUCKET = BUCKET_PESTO;
	
    public static final List<String> READ_BUCKETS = Arrays.asList(new String[] { BUCKET_PESTO, BUCKET_BOKDB, BUCKET_INITIAL });
	
    private Map<String,String> databases = new HashMap<String,String>();
	
    public DemoEditorResource() {
        databases.put("beer", "Beer database");
    }

    @Override
    protected PrestoSession createSession(String databaseId) {

        PojoSchemaProvider schemaProvider = PojoSchemaProvider.getSchemaProvider(databaseId, databaseId + ".presto.json");
        
        // schema stored in json and data stored in couchdb
        final CouchDataProvider dataProvider = new CouchDataProvider() {

            @Override
            protected CouchDbConnector createCouchDbConnector() {
                HttpClient httpClient = new StdHttpClient.Builder().build();
                CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
                return dbInstance.createConnector(DB_NAME, true);
            }

            @Override
            protected JacksonFieldStrategy createFieldStrategy(ObjectMapper mapper) {
                return new JacksonBucketFieldStrategy(mapper) {
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

        return new PojoSession(databaseId, getDatabaseName(databaseId), schemaProvider, dataProvider);
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
