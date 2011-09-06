package net.ontopia.presto.spi.impl.couchdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.CouchDbConnector;
import org.ektorp.DocumentNotFoundException;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.ViewResult.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CouchDataProvider implements PrestoDataProvider {

    private Logger log = LoggerFactory.getLogger(CouchDataProvider.class.getName());

    private final ObjectMapper mapper = new ObjectMapper();

    protected CouchDbConnector db;

    protected String designDocId = "_design/presto";

    public CouchDataProvider(CouchDbConnector db) {
        this.db = db;
    }

    protected CouchDbConnector getCouchConnector() {
        return db;
    }

    protected ObjectMapper getObjectMapper() {
        return mapper;
    }

    public String getProviderId() {
        return "couchdb";
    }

    public PrestoTopic getTopicById(String topicId) {
        // look up by document id
        ObjectNode doc = null;
        try {
            if (topicId != null) {
                doc = getCouchConnector().get(ObjectNode.class, topicId);
            }
        } catch (DocumentNotFoundException e) {
            log.warn("Topic with id '" + topicId + "' not found.");
        }
        return existing(doc);
    }

    public Collection<PrestoTopic> getTopicsByIds(Collection<String> topicIds) {
        if (topicIds.isEmpty()) {
            return Collections.emptyList();
        }
        Collection<PrestoTopic> result = new ArrayList<PrestoTopic>(topicIds.size());
        // look up by document ids
        ViewQuery query = new ViewQuery()
        .allDocs()
        .includeDocs(true).keys(topicIds);

        ViewResult viewResult = getCouchConnector().queryView(query);
        for (Row row : viewResult.getRows()) {
            JsonNode jsonNode = row.getDocAsNode();
            if (jsonNode.isObject()) {
                result.add(existing((ObjectNode)jsonNode));
            }
        }
        return result;
    }

    public Collection<PrestoTopic> getAvailableFieldValues(PrestoFieldUsage field) {
        if (field.isAddable()) {
            Collection<PrestoType> types = field.getAvailableFieldValueTypes();
            if (types.isEmpty()) {
                return Collections.emptyList();
            }
            List<String> typeIds = new ArrayList<String>();
            for (PrestoType type : types) {
                typeIds.add(type.getId());
            }
            List<PrestoTopic> result = new ArrayList<PrestoTopic>(typeIds.size());
            ViewQuery query = new ViewQuery()
            .designDocId(designDocId)
            .viewName("by-type").includeDocs(true).keys(typeIds);
    
            ViewResult viewResult = getCouchConnector().queryView(query);
            for (Row row : viewResult.getRows()) {
                ObjectNode doc = (ObjectNode)row.getDocAsNode();        
                result.add(existing(doc));
            }
            Collections.sort(result, new Comparator<PrestoTopic>() {
                public int compare(PrestoTopic o1, PrestoTopic o2) {
                    return compareComparables(o1.getName(), o2.getName());
                }
            });
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    protected int compareComparables(String o1, String o2) {
        if (o1 == null)
            return (o2 == null ? 0 : -1);
        else if (o2 == null)
            return 1;
        else
            return o1.compareTo(o2);
    }

    public PrestoChangeSet newChangeSet() {
        return new CouchChangeSet(this);
    }

    public void close() {
    }

    abstract CouchTopic existing(ObjectNode doc);

    abstract CouchTopic newInstance(PrestoType type);

}
