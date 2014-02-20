package net.ontopia.presto.spi.impl.riak;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.jackson.JacksonDataProvider;
import net.ontopia.presto.spi.jackson.JacksonTopic;

import org.codehaus.jackson.node.ObjectNode;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;

public abstract class RiakDataProvider extends JacksonDataProvider {

    private final IRiakClient riakClient;

    private final String bucketId;

    public RiakDataProvider(String bucketId) {
        this.bucketId = bucketId;
        this.riakClient = createRiakClient();
    }

    protected IRiakClient createRiakClient() {
        try {
            return RiakFactory.pbcClient();
        } catch (RiakException e) {
            throw new RuntimeException(e);
        }
    }

    // -- PrestoDataProvider
    
    @Override
    public String getProviderId() {
        return "riak";
    }

    @Override
    public PrestoTopic getTopicById(String topicId) {
        try {
            Bucket bucket = riakClient.fetchBucket(bucketId).execute();
            return existing(bucket.fetch(topicId, ObjectNode.class).execute());
        } catch (RiakRetryFailedException e) {
            throw new RuntimeException(e);
        }
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
    public Collection<? extends Object> getAvailableFieldValues(PrestoTopic topic, PrestoField field, String query) {
        // TODO: implement. use secondary indexes? or just leave it up to the users of this data provider.
        return Collections.emptyList();
    }

    @Override
    public void close() {
    }

    // -- DefaultDataProvider

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

}
