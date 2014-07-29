package net.ontopia.presto.spi.impl.couchdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.jackson.JacksonDataProvider;
import net.ontopia.presto.spi.jackson.JacksonTopic;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.Change;
import net.ontopia.presto.spi.utils.Utils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.CouchDbConnector;
import org.ektorp.DocumentNotFoundException;
import org.ektorp.DocumentOperationResult;
import org.ektorp.UpdateConflictException;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.ViewResult.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CouchDataProvider extends JacksonDataProvider {

    private static Logger log = LoggerFactory.getLogger(CouchDataProvider.class);

    static final int DEFAULT_OFFSET = 0;
    static final int DEFAULT_LIMIT = 100;

    private final CouchDbConnector db;

    protected String designDocId = "_design/presto";

    public CouchDataProvider() {
        this.db = createCouchDbConnector();
    }
    
    protected abstract CouchDbConnector createCouchDbConnector();
    
    protected CouchDbConnector getCouchConnector() {
        return db;
    }

    // -- PrestoDataProvider

    @Override
    public String getProviderId() {
        return "couchdb";
    }

    @Override
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

    @Override
    public Collection<PrestoTopic> getTopicsByIds(Collection<String> topicIds) {
        if (topicIds.isEmpty()) {
            return Collections.emptyList();
        }
        // look up by document ids
        ViewQuery query = new ViewQuery()
        .allDocs()
        .includeDocs(true).keys(topicIds);

        Collection<PrestoTopic> result = new ArrayList<PrestoTopic>(topicIds.size());
        ViewResult viewResult = getCouchConnector().queryView(query);
        for (Row row : viewResult.getRows()) {
            JsonNode docNode = row.getDocAsNode();
            if (docNode.isObject()) {
                result.add(existing((ObjectNode)docNode));
            }
        }
        return result;
    }

    @Override
    public Collection<? extends Object> getAvailableFieldValues(PrestoTopic topic, final PrestoField field, String query) {
        Collection<PrestoType> types = field.getAvailableFieldValueTypes();
        if (types.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> typeIds = new ArrayList<String>();
        for (PrestoType type : types) {
            typeIds.add(type.getId());
        }
        ViewQuery vquery = new ViewQuery()
        .designDocId(designDocId)
        .viewName("by-type")
        .staleOk(true)
        .includeDocs(true).keys(typeIds);

        List<PrestoTopic> result = new ArrayList<PrestoTopic>(typeIds.size());
        ViewResult viewResult = getCouchConnector().queryView(vquery);
        for (Row row : viewResult.getRows()) {
            ObjectNode docNode = (ObjectNode)row.getDocAsNode();
            if (docNode.isObject()) {
                result.add(existing(docNode));
            }
        }
        Collections.sort(result, new Comparator<PrestoTopic>() {
            @Override
            public int compare(PrestoTopic o1, PrestoTopic o2) {
                return Utils.compareComparables(o1.getName(field), o2.getName(field));
            }
        });
        return result;
    }

    @Override
    public void close() {
    }

    // -- DefaultDataProvider
    
    // this is here just to provide package visibility
    protected JacksonTopic existing(ObjectNode doc) {
        return super.existing(doc);
    }
    
    @Override
    public void create(PrestoTopic topic) {
        getCouchConnector().create(((JacksonTopic)topic).getData());
        log.info("Created: " + topic.getId() + " " + topic.getName());
    }

    @Override
    public void update(PrestoTopic topic) {
        getCouchConnector().update(((JacksonTopic)topic).getData());        
        log.info("Updated: " + topic.getId() + " " + topic.getName());
    }

    @Override
    public void updateBulk(List<Change> changes) {
        List<ObjectNode> bulkDocuments = new ArrayList<ObjectNode>();
        for (Change change : changes) {
            if (change.isTopicUpdated()) {
                JacksonTopic topic = (JacksonTopic)change.getTopic();

                if (change.getType().equals(Change.Type.DELETE)) {
                    ObjectNode data = topic.getData();
                    data.put("_deleted", true);
                }

                log.info("Bulk update document: " + change.getType() + " " + topic.getId());
                bulkDocuments.add(topic.getData());
            }
        }
        for (DocumentOperationResult dor : getCouchConnector().executeAllOrNothing(bulkDocuments)) {
            log.warn("Bulk update error (probably caused conflict): " + dor);
        }
    }

    @Override
    public boolean delete(PrestoTopic topic) {
        log.info("Removing: " + topic.getId() + " " + topic.getName());
        try {
            getCouchConnector().delete(((JacksonTopic)topic).getData());
            return true;
        } catch (UpdateConflictException e) {
            JacksonTopic topic2 = (JacksonTopic)getTopicById(topic.getId());
            if (topic2 != null) {
                getCouchConnector().delete(topic2.getData());
                return true;
            } else {
                return false;
            }
        }
    }

    // builder pattern

    public CouchDataProvider designDocId(String designDocId) {    
        this.designDocId = designDocId;
        return this;
    }

}
