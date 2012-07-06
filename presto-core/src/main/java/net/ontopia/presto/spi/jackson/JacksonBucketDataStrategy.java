package net.ontopia.presto.spi.jackson;

import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoFieldUsage;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public abstract class JacksonBucketDataStrategy implements JacksonDataStrategy {

    protected static final String ID_DEFAULT_FIELD = "_id";
    protected static final String TYPE_DEFAULT_FIELD = ":type";
    protected static final String NAME_DEFAULT_FIELD = ":name";
    
    private final ObjectMapper mapper;

    public JacksonBucketDataStrategy(ObjectMapper mapper) {
        this.mapper = mapper;
    }
    
    protected ObjectMapper getObjectMapper() {
        return mapper;
    }
    
    protected abstract List<String> getReadBuckets(ObjectNode doc);

    protected abstract String getWriteBucket(ObjectNode doc);
    
    @Override
    public String getId(ObjectNode doc) {
        JsonNode node = doc.get(ID_DEFAULT_FIELD);
        return node == null ? null : node.getTextValue();
    }
    
    @Override
    public String getTypeId(ObjectNode doc) {
        return doc.get(TYPE_DEFAULT_FIELD).getTextValue();
    }
    
    @Override
    public String getName(ObjectNode doc) {
        JsonNode node = doc.get(NAME_DEFAULT_FIELD);
        return node == null ? null : node.getTextValue();
    }
    
    @Override
    public String getName(ObjectNode doc, PrestoFieldUsage field) {
        return getName(doc);
    }

    @Override
    public ArrayNode getFieldValue(ObjectNode doc, PrestoField field) {
        return getBucketFieldValue(field.getActualId(), getReadBucket(doc, field, true));
    }
    
    protected ArrayNode getFieldValue(ObjectNode doc, String fieldId) {
        return getBucketFieldValue(fieldId, getReadBucket(doc, fieldId, true));
    }

    @Override
    public void putFieldValue(ObjectNode doc, PrestoField field, ArrayNode value) {
        putFieldValue(doc, field.getActualId(), value);
    }
       
    protected void putFieldValue(ObjectNode doc, String fieldId, ArrayNode value) {
        ObjectNode writeBucket = getWriteBucket(doc, fieldId, true);
        ObjectNode readBucket = getReadBucket(doc, fieldId, false);
        // remove write bucket field iff value equal to value in closest read bucket
        if (readBucket != null && equalValues(value, getBucketFieldValue(fieldId, readBucket))) {
            if (writeBucket.has(fieldId)) {        
                writeBucket.remove(fieldId);
            }
        } else {
            writeBucket.put(fieldId, value);
        }
    }

    protected ArrayNode getBucketFieldValue(String fieldId, ObjectNode bucketData) {
        if (bucketData != null) {
            JsonNode value = bucketData.get(fieldId);
            return (ArrayNode)(value != null && value.isArray() ? value : null);
        }
        return null;
    }
    
    protected ObjectNode getReadBucket(ObjectNode doc, PrestoField field, boolean includeWriteBucket) {
        return getReadBucket(doc, field.getActualId(), includeWriteBucket);
    }
    
    protected ObjectNode getReadBucket(ObjectNode doc, String fieldId, boolean includeWriteBucket) {
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
                if (bucket.has(fieldId)) {
                    return bucket;
                }
            }
        }
        return null;    
    }

    protected ObjectNode getWriteBucket(ObjectNode doc, PrestoField field, boolean create) {
        return getWriteBucket(doc, field.getActualId(), create);
    }
           
    protected ObjectNode getWriteBucket(ObjectNode doc, String fieldId, boolean create) {
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

    // -- convenience methods

    protected boolean equalValues(Object o1, Object o2) {
        if (o1 == null)
            return (o2 == null ? true : false);
        else if (o2 == null)
            return false;
        else
            return o1.equals(o2);
    }

    protected String getFirstStringValue(ObjectNode doc, String fieldId) {
        ArrayNode node = getFieldValue(doc, fieldId);
        if (node != null && node.size() > 0) {
            return node.get(0).getTextValue();
        }
        return null;
    }
    
}
