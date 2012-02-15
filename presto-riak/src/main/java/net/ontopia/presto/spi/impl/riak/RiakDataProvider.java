package net.ontopia.presto.spi.impl.riak;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.jackson.JacksonDataProvider;
import net.ontopia.presto.spi.jackson.JacksonDataStrategy;
import net.ontopia.presto.spi.jackson.JacksonTopic;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.Change;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.DefaultTopic;
import net.ontopia.presto.spi.utils.PrestoFieldResolver;
import net.ontopia.presto.spi.utils.PrestoFunctionResolver;
import net.ontopia.presto.spi.utils.PrestoTraverseResolver;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;

public abstract class RiakDataProvider implements JacksonDataProvider {

    private static Logger log = LoggerFactory.getLogger(RiakDataProvider.class.getName());

    private final IRiakClient riakClient;
    private final ObjectMapper mapper;
    private final JacksonDataStrategy dataStrategy;

    private final String bucketId;

    public RiakDataProvider(String bucketId) {
        this.bucketId = bucketId;
        this.riakClient = createRiakClient();
        this.mapper = createObjectMapper();
        this.dataStrategy = createDataStrategy(mapper);  
    }

    protected IRiakClient createRiakClient() {
        try {
            return RiakFactory.pbcClient();
        } catch (RiakException e) {
            throw new RuntimeException(e);
        }
    }
    
    protected ObjectMapper createObjectMapper() {
        return new ObjectMapper();
    }

    abstract protected JacksonDataStrategy createDataStrategy(ObjectMapper mapper);

    // -- PrestoDataProvider
    
    @Override
    public String getProviderId() {
        return "riak";
    }

    @Override
    public PrestoTopic getTopicById(String id) {
        try {
            Bucket bucket = riakClient.fetchBucket(bucketId).execute();
            return existing(bucket.fetch(id, ObjectNode.class).execute());
        } catch (RiakRetryFailedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<PrestoTopic> getTopicsByIds(Collection<String> ids) {
        Collection<PrestoTopic> result = new ArrayList<PrestoTopic>();
        for (String id : ids) {
            PrestoTopic topic = getTopicById(id);
            if (topic != null) {
                result.add(topic);
            }
        }
        return null;
    }

    @Override
    public Collection<PrestoTopic> getAvailableFieldValues(PrestoFieldUsage field) {
        return Collections.emptyList();
    }

    @Override
    public PrestoChangeSet newChangeSet() {
        return new PrestoDefaultChangeSet(this);
    }

    @Override
    public void close() {
    }

    // -- DefaultDataProvider
    
    protected JacksonTopic existing(ObjectNode doc) {
        return doc == null ? null : new JacksonTopic(this, doc);
    }

    @Override
    public DefaultTopic newInstance(PrestoType type) {
        ObjectNode doc = getObjectMapper().createObjectNode();
        doc.put(":type", type.getId());
        return new JacksonTopic(this, doc);
    }

    @Override
    public void create(PrestoTopic topic) {
        try {
            ObjectNode data = ((JacksonTopic)topic).getData();

            String topicId = createNewTopicId(topic);
            data.put("_id", topicId);
            
            Bucket bucket = riakClient.createBucket(bucketId).execute();            
            bucket.store(topicId, data).execute();
        } catch (RiakRetryFailedException e) {
            throw new RuntimeException(e);
        }
    }

    protected String createNewTopicId(PrestoTopic topic) {
        return UUID.randomUUID().toString();
    }
    
    @Override
    public void update(PrestoTopic topic) {
        try {
            ObjectNode data = ((JacksonTopic)topic).getData();
            
            Bucket bucket = riakClient.fetchBucket(bucketId).execute();
            bucket.store(topic.getId(), data).execute();
        } catch (RiakRetryFailedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean delete(PrestoTopic topic) {
        try {
            Bucket bucket = riakClient.fetchBucket(bucketId).execute();
            bucket.delete(topic.getId()).execute();
            return true;
        } catch (RiakException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateBulk(List<Change> changes) {
        for (Change c : changes) {
            switch (c.getType()) {
            case CREATE:
                create(c.getTopic());
                break;
            case UPDATE:
                update(c.getTopic());
                break;
            case DELETE:
                delete(c.getTopic());
                break;
            }
        }
    }

    // -- JacksonDataProvider
    
    @Override
    public ObjectMapper getObjectMapper() {
        return mapper;
    }

    @Override
    public JacksonDataStrategy getDataStrategy() {
        return dataStrategy;
    }

    @Override
    public PrestoFieldResolver createFieldResolver(PrestoSchemaProvider schemaProvider, ObjectNode config) {
        PrestoContext context = new PrestoContext(this, schemaProvider, getObjectMapper());
        String type = config.get("type").getTextValue();
        if (type == null) {
            log.error("type not specified on resolve item: " + config);
        } else if (type.equals("traverse")) {
            return new PrestoTraverseResolver(context, config);
        } else if (type.equals("function")) {
            return new PrestoFunctionResolver(context, config);
        } else {
            log.error("Unknown type specified on resolve item: " + config);            
        }
        return null;
    }

}
