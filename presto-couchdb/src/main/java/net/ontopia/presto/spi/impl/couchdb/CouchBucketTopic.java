package net.ontopia.presto.spi.impl.couchdb;

import net.ontopia.presto.spi.PrestoField;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public class CouchBucketTopic extends CouchTopic {

    protected CouchBucketTopic(CouchBucketDataProvider dataProvider, ObjectNode data) {
        super(dataProvider, data);
    }

    @Override
    protected CouchBucketDataProvider getDataProvider() {
        return (CouchBucketDataProvider)super.getDataProvider();
    }

    // json field data access strategy

    @Override
    protected ArrayNode getFieldValue(PrestoField field) {
        return getBucketFieldValue(field, getReadBucket(field, true));
    }

    protected ArrayNode getBucketFieldValue(PrestoField field, ObjectNode bucketData) {
        if (bucketData != null) {
            JsonNode value = bucketData.get(field.getActualId());
            return (ArrayNode)(value != null && value.isArray() ? value : null);
        }
        return null;
    }

    @Override
    protected void putFieldValue(PrestoField field, ArrayNode value) {
        ObjectNode writeBucket = getWriteBucket(field, true);
        ObjectNode readBucket = getReadBucket(field, false);
        // remove write bucket field iff value equal to value in closest read bucket
        if (readBucket != null && equalValues(value, getBucketFieldValue(field, readBucket))) {
            if (writeBucket.has(field.getActualId())) {        
                writeBucket.remove(field.getActualId());
            }
        } else {
            writeBucket.put(field.getActualId(), value);
        }
    }

    protected boolean equalValues(Object o1, Object o2) {
        if (o1 == null)
            return (o2 == null ? true : false);
        else if (o2 == null)
            return false;
        else
            return o1.equals(o2);
    }

    protected ObjectNode getReadBucket(PrestoField field, boolean includeWriteBucket) {
        ObjectNode data = getData();
        // find the right bucket
        for (String bucketId : getDataProvider().getReadBuckets()) {
            // ignore write bucket
            if (!includeWriteBucket && bucketId.equals(getDataProvider().getWriteBucket())) {
                continue;
            }
            // return bucket if it exists and contains field
            if (data.has(bucketId)) {
                ObjectNode bucket = (ObjectNode)data.get(bucketId);
                if (bucket.has(field.getActualId())) {
                    return bucket;
                }
            }
        }
        return null;    
    }

    protected ObjectNode getWriteBucket(PrestoField field, boolean create) {
        String writeBucket = getDataProvider().getWriteBucket();
        ObjectNode data = getData();
        ObjectNode bucket = null;
        if (data.has(writeBucket)) {
            bucket = (ObjectNode)data.get(writeBucket);
        }
        if (bucket == null && create) {
            bucket = getDataProvider().getObjectMapper().createObjectNode();
            data.put(writeBucket, bucket);
        }
        return bucket;
    }

}
