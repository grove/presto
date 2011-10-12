package net.ontopia.presto.spi.impl.couchdb;

import java.util.List;

import net.ontopia.presto.spi.PrestoField;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public abstract class CouchBucketFieldStrategy implements CouchFieldStrategy {

    protected abstract ObjectMapper getObjectMapper();
    
    protected abstract List<String> getReadBuckets(ObjectNode doc);

    protected abstract String getWriteBucket(ObjectNode doc);
    
    @Override
    public ArrayNode getFieldValue(ObjectNode doc, PrestoField field) {
        return getBucketFieldValue(field, getReadBucket(doc, field, true));
    }

    @Override
    public void putFieldValue(ObjectNode doc, PrestoField field, ArrayNode value) {
        ObjectNode writeBucket = getWriteBucket(doc, field, true);
        ObjectNode readBucket = getReadBucket(doc, field, false);
        // remove write bucket field iff value equal to value in closest read bucket
        if (readBucket != null && equalValues(value, getBucketFieldValue(field, readBucket))) {
            if (writeBucket.has(field.getActualId())) {        
                writeBucket.remove(field.getActualId());
            }
        } else {
            writeBucket.put(field.getActualId(), value);
        }
    }

    protected ArrayNode getBucketFieldValue(PrestoField field, ObjectNode bucketData) {
        if (bucketData != null) {
            JsonNode value = bucketData.get(field.getActualId());
            return (ArrayNode)(value != null && value.isArray() ? value : null);
        }
        return null;
    }

    protected boolean equalValues(Object o1, Object o2) {
        if (o1 == null)
            return (o2 == null ? true : false);
        else if (o2 == null)
            return false;
        else
            return o1.equals(o2);
    }
    
    protected ObjectNode getReadBucket(ObjectNode doc, PrestoField field, boolean includeWriteBucket) {
        String writeBucket = getWriteBucket(doc);
        // find the right bucket
        for (String bucketId : getReadBuckets(doc)) {
            // ignore write bucket
            if (!includeWriteBucket && bucketId.equals(writeBucket)) {
                continue;
            }
            // return bucket if it exists and contains field
            if (doc.has(bucketId)) {
                ObjectNode bucket = (ObjectNode)doc.get(bucketId);
                if (bucket.has(field.getActualId())) {
                    return bucket;
                }
            }
        }
        return null;    
    }

    protected ObjectNode getWriteBucket(ObjectNode doc, PrestoField field, boolean create) {
        String writeBucket = getWriteBucket(doc);
        ObjectNode bucket = null;
        if (doc.has(writeBucket)) {
            bucket = (ObjectNode)doc.get(writeBucket);
        }
        if (bucket == null && create) {
            bucket = getObjectMapper().createObjectNode();
            doc.put(writeBucket, bucket);
        }
        return bucket;
    }

}
