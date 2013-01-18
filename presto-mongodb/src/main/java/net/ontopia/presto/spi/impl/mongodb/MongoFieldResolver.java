package net.ontopia.presto.spi.impl.mongodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.utils.PrestoFieldResolver;
import net.ontopia.presto.spi.utils.PrestoPagedValues;
import net.ontopia.presto.spi.utils.PrestoVariableContext;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;
import net.vz.mongodb.jackson.JacksonDBCollection;
import net.vz.mongodb.jackson.MongoJsonMappingException;
import net.vz.mongodb.jackson.internal.object.BsonObjectGenerator;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

public abstract class MongoFieldResolver extends PrestoFieldResolver {

    private static Logger log = LoggerFactory.getLogger(MongoFieldResolver.class);

    private final ObjectMapper mapper = new ObjectMapper();

    protected abstract DB getDB();

    protected abstract PrestoTopic existingTopic(ObjectNode doc);

    @Override
    public PagedValues resolve(Collection<? extends Object> objects,
            PrestoField field, boolean isReference, Paging paging, PrestoVariableResolver variableResolver) {

        PrestoVariableContext context = getVariableContext();
        ObjectNode config = getConfig();

        try {
            DB db = getDB();
            
            String coll = config.path("coll").getTextValue();
            DBCollection collection = db.getCollection(coll);

            JsonNode qNode = config.path("q");

            Collection<JsonNode> qvalues = context.replaceVariables(variableResolver, objects, qNode);
            DBObject q;
            if (qvalues.isEmpty()) {
                return new PrestoPagedValues(Collections.emptyList(), paging, 0);
            } else if (qvalues.size() == 1) {
                q = convertToDbObject(replaceKeywords(qvalues.iterator().next()));
            } else {
                BasicDBList qAlternatives = new BasicDBList();
                Iterator<JsonNode> iter = qvalues.iterator();
                while (iter.hasNext()) {
                    qAlternatives.add(convertToDbObject(replaceKeywords(iter.next())));
                }
                q = new BasicDBObject("$or", qAlternatives);
            }
            JacksonDBCollection<ObjectNode, String> jCollection = JacksonDBCollection.wrap(collection, ObjectNode.class, String.class);
            net.vz.mongodb.jackson.DBCursor<ObjectNode> cursor;

            JsonNode keysNode = config.path("keys");
            if (keysNode.isObject()) {
                DBObject keys = convertToDbObject(keysNode);
                log.debug("Q: {} keys: {}", q, keys);
                cursor = jCollection.find(q, keys);            
            } else {
                log.debug("Q: {}", q);
                cursor = jCollection.find(q);
            }
            
            try {
                int total = cursor.size();
                List<Object> result = new ArrayList<Object>(total);

                while (cursor.hasNext()) {
                    ObjectNode next = cursor.next();

                    if (isReference) {
                        result.add(existingTopic(next));
                    } else if (keysNode.isObject()){
                        Iterator<String> fieldNames = keysNode.getFieldNames();
                        while (fieldNames.hasNext()) {
                            for (JsonNode valueNode : next.path(fieldNames.next())) {
                                if (valueNode.isTextual()) {
                                    result.add(valueNode.getTextValue());
                                }
                            }
                        }
                    } else {
                        String valueId = next.get("_id").getTextValue();
                        result.add(valueId);
                    }
                }
                return new PrestoPagedValues(result, paging, total);
            } finally {
                cursor.close();
            }
        } catch (Exception e) {
            log.error("QE: " + config, e);
            return new PrestoPagedValues(Collections.emptyList(), paging, 0);
        }
    }

    protected JsonNode replaceKeywords(JsonNode o) throws MongoException {
        if (o == null) {
            return null;
        } else if (o.isArray()) {
            ArrayNode list = mapper.createArrayNode();
            for (JsonNode v : o) {
                list.add(replaceKeywords(v));
            }
            return list;
        } else if (o.isObject()) {
            ObjectNode obj = mapper.createObjectNode();
            Iterator<String> iter = o.getFieldNames();
            while (iter.hasNext()) {
                String fieldName = iter.next();
                if (fieldName.startsWith("$$")) {
                    obj.put(fieldName.substring(1), replaceKeywords(o.get(fieldName)));
                } else {
                    obj.put(fieldName, replaceKeywords(o.get(fieldName)));                    
                }
            }
            return obj;
        } else {
            return o;
        }
    }
    
    protected DBObject convertToDbObject(JsonNode object) throws MongoException {
        if (object == null) {
            return null;
        }
        BsonObjectGenerator generator = new BsonObjectGenerator();
        try {
            mapper.writeValue(generator, object);
        } catch (JsonMappingException e) {
            throw new MongoJsonMappingException(e);
        } catch (IOException e) {
            // This shouldn't happen
            throw new MongoException("Unknown error occurred converting BSON to object", e);
        }
        return generator.getDBObject();
    }

}
