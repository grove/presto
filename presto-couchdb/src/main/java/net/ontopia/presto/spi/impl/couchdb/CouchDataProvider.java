package net.ontopia.presto.spi.impl.couchdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.CouchDbConnector;
import org.ektorp.DocumentNotFoundException;
import org.ektorp.UpdateConflictException;
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

    public PrestoChangeSet createTopic(PrestoType type) {
        return new CouchChangeSet(this, type);
    }

    public PrestoChangeSet updateTopic(PrestoTopic topic, PrestoType type) {
        return new CouchChangeSet(this, (CouchTopic)topic, type);
    }

    public boolean deleteTopic(PrestoTopic topic, PrestoType type) {
        return deleteTopic(topic, type, true);
    }

    private boolean deleteTopic(PrestoTopic topic, PrestoType type, boolean removeDependencies) {
        // find and remove dependencies
        if (removeDependencies) {
            removeDependencies(topic, type);
        }
        // clear incoming foreign keys
        for (PrestoField field : type.getFields()) {
            if (field.getInverseFieldId() != null) {
                boolean isNew = false;
                removeInverseFieldValue(isNew, topic, field, topic.getValues(field));
            }
        }

        return delete((CouchTopic)topic);    
    }

    private void removeDependencies(PrestoTopic topic, PrestoType type) {
        PrestoSchemaProvider schemaProvider = type.getSchemaProvider();
        for (PrestoTopic dependency : findDependencies(topic, type)) {
            if (!dependency.equals(topic)) {
                PrestoType dependencyType = schemaProvider.getTypeById(dependency.getTypeId());
                deleteTopic(dependency, dependencyType, false);
            }
        }
    }

    public void close() {
    }

    abstract CouchTopic existing(ObjectNode doc);

    abstract CouchTopic newInstance(PrestoType type);

    // couchdb crud operations

    void create(CouchTopic topic) {
        getCouchConnector().create(topic.getData());
    }

    void update(CouchTopic topic) {
        getCouchConnector().update(topic.getData());
    }

    boolean delete(CouchTopic topic) {
        log.info("Removing: " + topic.getId() + " " + topic.getName());
        try {
            getCouchConnector().delete(topic.getData());
            return true;
        } catch (UpdateConflictException e) {
            CouchTopic topic2 = (CouchTopic)getTopicById(topic.getId());
            if (topic2 != null) {
                getCouchConnector().delete(topic2.getData());
                return true;
            } else {
                return false;
            }
        }
    }

    // dependent topics / cascading deletes

    private Collection<PrestoTopic> findDependencies(PrestoTopic topic, PrestoType type) {
        Collection<PrestoTopic> dependencies = new HashSet<PrestoTopic>();
        findDependencies(topic, type, dependencies);
        return dependencies;
    }

    private void findDependencies(PrestoTopic topic, PrestoType type, Collection<PrestoTopic> deleted) {

        if (!deleted.contains(topic) && type.isRemovableCascadingDelete()) {
            for (PrestoField field : type.getFields()) {
                if (field.isReferenceField()) {
                    if (field.isCascadingDelete()) { 
                        PrestoSchemaProvider schemaProvider = type.getSchemaProvider();
                        for (Object value : topic.getValues(field)) {
                            PrestoTopic valueTopic = (PrestoTopic)value;
                            String typeId = valueTopic.getTypeId();
                            PrestoType valueType = schemaProvider.getTypeById(typeId);
                            deleted.add(valueTopic);
                            findDependencies(valueTopic, valueType, deleted);
                        }
                    }
                }
            }
        }
    }

    // inverse fields (foreign keys)

    void addInverseFieldValue(boolean isNew, PrestoTopic topic, PrestoField field, Collection<?> values) {
        String inverseFieldId = field.getInverseFieldId();
        if (inverseFieldId != null) {
            for (Object value : values) {

                CouchTopic valueTopic = (CouchTopic)value;
                PrestoType type = field.getSchemaProvider().getTypeById(valueTopic.getTypeId());
                PrestoField inverseField = type.getFieldById(inverseFieldId);

                int index = -1;
                valueTopic.addValue(inverseField, Collections.singleton(topic), index);
                update(valueTopic);      
            }
        }
    }

    void removeInverseFieldValue(boolean isNew, PrestoTopic topic, PrestoField field, Collection<?> values) {
        if (!isNew) {
            String inverseFieldId = field.getInverseFieldId();
            if (inverseFieldId != null) {
                for (Object value : values) {

                    CouchTopic valueTopic = (CouchTopic)value;
                    PrestoType valueType = field.getSchemaProvider().getTypeById(valueTopic.getTypeId());
                    if (field.isCascadingDelete() && valueType.isRemovableCascadingDelete()) {
                        deleteTopic(valueTopic, valueType);
                    } else {          
                        PrestoField inverseField = valueType.getFieldById(inverseFieldId);
                        valueTopic.removeValue(inverseField, Collections.singleton(topic));
                        update(valueTopic);
                    }
                }
            }
        }
    }

}
