package net.ontopia.presto.spi.jackson;

import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.utils.ExtraUtils;
import net.ontopia.presto.spi.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class JacksonBucketDataStrategy implements JacksonDataStrategy {
    
    private static Logger log = LoggerFactory.getLogger(JacksonBucketDataStrategy.class);

    protected static final String ID_DEFAULT_FIELD = "_id";
    protected static final String TYPE_DEFAULT_FIELD = ":type";
    protected static final String NAME_DEFAULT_FIELD = ":name";
    
    private final ObjectMapper mapper;

    public JacksonBucketDataStrategy(ObjectMapper mapper) {
        this.mapper = mapper;
    }
    
    public ObjectMapper getObjectMapper() {
        return mapper;
    }
    
    public abstract List<String> getReadBucketIds(ObjectNode doc);

    public abstract String getWriteBucketId(ObjectNode doc);
    
    @Override
    public String getId(ObjectNode doc) {
        JsonNode node = doc.get(ID_DEFAULT_FIELD);
        return node == null ? null : node.textValue();
    }
    
    @Override
    public String getTypeId(ObjectNode doc) {
        return doc.get(TYPE_DEFAULT_FIELD).textValue();
    }
    
    @Override
    public String getName(ObjectNode doc) {
        JsonNode node = doc.get(NAME_DEFAULT_FIELD);
        return node == null ? null : node.textValue();
    }
    
    @Override
    public String getName(ObjectNode doc, PrestoField field) {
        return getName(doc);
    }

    @Override
    public boolean hasFieldValue(ObjectNode doc, PrestoField field) {
        String fieldId = field.getActualId();
        ObjectNode readBucket = getReadBucket(doc, fieldId, true);
        return getBucketFieldValue(fieldId, readBucket) != null;
    }

    @Override
    public ArrayNode getFieldValue(ObjectNode doc, PrestoField field) {
        ObjectNode extra = ExtraUtils.getFieldExtraNode(field);
        if (extra != null) {
            JsonNode readStrategy = extra.path("readStrategy");
            if (readStrategy.isObject()) {
                String className = readStrategy.path("class").textValue();
                if (className != null) {
                    JacksonFieldDataStrategy fds = Utils.newInstanceOf(className, JacksonFieldDataStrategy.class, true);
                    if (fds != null) {
                        fds.setJacksonDataStrategy(this);
                        fds.setConfig((ObjectNode)readStrategy);
                        return fds.getFieldValue(doc, field);
                    }
                } else {
                    log.warn("Not able to find read strategy from configuration: " + extra);
                }
            }
        }
        String fieldId = field.getActualId();
        return getFieldValuesDefault(doc, fieldId);
    }
    
    protected ArrayNode getFieldValuesDefault(ObjectNode doc, String fieldId) {
        // Strategy: default: value of closest read bucket with value in field
        ObjectNode readBucket = getReadBucket(doc, fieldId, true);
        return getBucketFieldValue(fieldId, readBucket);
    }
    
    @Override
    public void putFieldValue(ObjectNode doc, PrestoField field, ArrayNode value) {
        String fieldId = field.getActualId();
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
    
    @Override
    public void clearFieldValue(ObjectNode doc, PrestoField field) {
        String fieldId = field.getActualId();
        ObjectNode writeBucket = getWriteBucket(doc, field, false);
        if (writeBucket != null) {
            writeBucket.remove(fieldId);
        }
    }
    
    public ObjectNode getBucket(String bucketId, ObjectNode doc) {
        return (ObjectNode)doc.get(bucketId);
    }
    
    public ArrayNode getBucketFieldValue(String fieldId, ObjectNode bucketData) {
        if (bucketData != null) {
            return getFieldValueArrayNode(bucketData, fieldId);
        }
        return null;
    }
    
    private ArrayNode getFieldValueArrayNode(ObjectNode doc, String fieldId) {
        JsonNode value = doc.path(fieldId);
        if (value.isArray()) {
            return (ArrayNode)value;
        } else if (value.isMissingNode() || value.isNull()) {
            return null;
        } else {
            log.warn("Value " + value + " in field '" + fieldId + "' is not an array");
            return null;
        }
    }
    
    protected ObjectNode getReadBucket(ObjectNode doc, PrestoField field, boolean includeWriteBucket) {
        return getReadBucket(doc, field.getActualId(), includeWriteBucket);
    }
    
    protected ObjectNode getReadBucket(ObjectNode doc, String fieldId, boolean includeWriteBucket) {
        String writeBucket = getWriteBucketId(doc);
        // find the right bucket
        for (String bucketId : getReadBucketIds(doc)) {
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
        String writeBucket = getWriteBucketId(doc);
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
    
}
