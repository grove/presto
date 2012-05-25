package net.ontopia.presto.spi.impl.couchdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoChanges;
import net.ontopia.presto.spi.PrestoField;
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

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
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

public abstract class CouchDataProvider implements JacksonDataProvider {

    private static Logger log = LoggerFactory.getLogger(CouchDataProvider.class.getName());

    static final int DEFAULT_OFFSET = 0;
    static final int DEFAULT_LIMIT = 100;

    private final CouchDbConnector db;
    private final ObjectMapper mapper;
    private final JacksonDataStrategy dataStrategy;

    protected String designDocId = "_design/presto";

    public CouchDataProvider() {
        this.db = createCouchDbConnector();
        this.mapper = createObjectMapper();
        this.dataStrategy = createDataStrategy(mapper);
    }
    
    protected abstract CouchDbConnector createCouchDbConnector();
    
    protected CouchDbConnector getCouchConnector() {
        return db;
    }
    
    protected ObjectMapper createObjectMapper() {
        return new ObjectMapper();
    }

    abstract protected JacksonDataStrategy createDataStrategy(ObjectMapper mapper);

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
    public Collection<PrestoTopic> getAvailableFieldValues(final PrestoFieldUsage field) {
        if (field.isAddable()) {
            Collection<PrestoType> types = field.getAvailableFieldValueTypes();
            if (types.isEmpty()) {
                return Collections.emptyList();
            }
            List<String> typeIds = new ArrayList<String>();
            for (PrestoType type : types) {
                typeIds.add(type.getId());
            }
            ViewQuery query = new ViewQuery()
            .designDocId(designDocId)
            .viewName("by-type")
            .staleOk(true)
            .includeDocs(true).keys(typeIds);
    
            List<PrestoTopic> result = new ArrayList<PrestoTopic>(typeIds.size());
            ViewResult viewResult = getCouchConnector().queryView(query);
            for (Row row : viewResult.getRows()) {
                ObjectNode docNode = (ObjectNode)row.getDocAsNode();
                if (docNode.isObject()) {
                    result.add(existing(docNode));
                }
            }
            Collections.sort(result, new Comparator<PrestoTopic>() {
                @Override
                public int compare(PrestoTopic o1, PrestoTopic o2) {
                    return compareComparables(o1.getName(field), o2.getName(field));
                }
            });
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    private final int compareComparables(String o1, String o2) {
        if (o1 == null)
            return (o2 == null ? 0 : -1);
        else if (o2 == null)
            return 1;
        else
            return o1.compareTo(o2);
    }

    @Override
    public PrestoChangeSet newChangeSet() {
        return new PrestoDefaultChangeSet(this) {
            @Override
            protected void onBeforeSave() {
                CouchDataProvider.this.onBeforeSave(this, this.getPrestoChanges());
            }
        };
    }

    /**
     * Override this method to do further updates before the changeset is saved.
     */
    protected void onBeforeSave(PrestoChangeSet changeSet, PrestoChanges changes) {
    }
    

    // assign initial values
    protected List<? extends Object> getInitialValues(PrestoField field) {
        return Collections.emptyList();
    }

    @Override
    public void close() {
    }

    // builder pattern

    public CouchDataProvider designDocId(String designDocId) {    
        this.designDocId = designDocId;
        return this;
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
        } else if (type.equals("couchdb-view")) {
            return new CouchViewResolver(this, context, config);
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
