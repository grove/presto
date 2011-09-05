package net.ontopia.presto.spi.impl.couchdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;

import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.CouchDbConnector;
import org.ektorp.DocumentOperationResult;
import org.ektorp.UpdateConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CouchChangeSet implements PrestoChangeSet {

    private Logger log = LoggerFactory.getLogger(CouchChangeSet.class.getName());

    static interface CouchChange {
        enum Type {CREATE, UPDATE, DELETE};
        Type getType();
        void applyUpdate();
        CouchTopic getTopic();
    }

    class CouchDelete implements CouchChange {
        private final CouchTopic topic;
        private CouchDelete(CouchTopic topic) {
            this.topic = topic;
        }
        public Type getType() {
            return Type.DELETE;
        }
        public void applyUpdate() {
            ObjectNode data = topic.getData();
            data.put("_deleted", true);
        }
        public CouchTopic getTopic() {
            return topic;
        }
    }

    private final CouchDataProvider dataProvider;

    private final List<CouchChange> changes = new ArrayList<CouchChange>();

    private boolean saved;

    CouchChangeSet(CouchDataProvider dataProvider) {
        this.dataProvider = dataProvider;
    }

    CouchTopic newInstance(PrestoType type) {
        return dataProvider.newInstance(type);
    }

    public PrestoUpdate createTopic(PrestoType type) {
        CouchUpdate update = new CouchUpdate(this, type);
        changes.add(update);
        return update;
    }

    public PrestoUpdate updateTopic(PrestoTopic topic, PrestoType type) {
        CouchUpdate update = new CouchUpdate(this, (CouchTopic)topic, type);
        changes.add(update);
        return update;
    }

    public void deleteTopic(PrestoTopic topic, PrestoType type) {
        deleteTopic(topic, type, true);
    }

    private void deleteTopic(PrestoTopic topic, PrestoType type, boolean removeDependencies) {
        // find and remove dependencies
        if (removeDependencies) {
            removeDependencies(topic, type);
        }
        //        // clear incoming foreign keys
        //        for (PrestoField field : type.getFields()) {
        //            if (field.getInverseFieldId() != null) {
        //                boolean isNew = false;
        //                removeInverseFieldValue(isNew, topic, field, topic.getValues(field));
        //            }
        //        }

        changes.add(new CouchDelete((CouchTopic)topic));
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

    // dependent topics / cascading deletes

    private Collection<PrestoTopic> findDependencies(PrestoTopic topic, PrestoType type) {
        Collection<PrestoTopic> dependencies = new HashSet<PrestoTopic>();
        findDependencies(topic, type, dependencies);
        return dependencies;
    }

    private void findDependencies(PrestoTopic topic, PrestoType type, Collection<PrestoTopic> dependencies) {

        for (PrestoField field : type.getFields()) {
            if (field.isReferenceField() && field.isCascadingDelete()) { 
                PrestoSchemaProvider schemaProvider = type.getSchemaProvider();
                for (Object value : topic.getValues(field)) {
                    PrestoTopic valueTopic = (PrestoTopic)value;
                    String typeId = valueTopic.getTypeId();
                    PrestoType valueType = schemaProvider.getTypeById(typeId);
                    if (valueType.isRemovableCascadingDelete()) {
                        if (!dependencies.contains(valueTopic)) {
                            dependencies.add(valueTopic);
                            findDependencies(valueTopic, valueType, dependencies);
                        }
                    }
                }
            }
        }
    }
    
    public void save() {
        if (saved) {
            log.warn("PrestoChangeSet.save() method called multiple times.");
            return; // idempotent
        }
        this.saved = true;
        CouchChange[] changesArray = changes.toArray(new CouchChange[changes.size()]);
        for (CouchChange change : changesArray) {
            change.applyUpdate();
        }
        if (changes.size() == 1) {
            CouchChange change = changes.get(0);
            if (change.getType().equals(CouchChange.Type.CREATE)) {
                create(change.getTopic());                
            } else if (change.getType().equals(CouchChange.Type.UPDATE)) {
                update(change.getTopic());                
            } else if (change.getType().equals(CouchChange.Type.DELETE)) {
                delete(change.getTopic());                
            }
        } else if (changes.size() > 1) {
            CouchDbConnector couchConnector = dataProvider.getCouchConnector();
            List<ObjectNode> bulkDocuments = new ArrayList<ObjectNode>();
            for (CouchChange change : changes) {
                CouchTopic topic = change.getTopic();
                log.info("Bulk update document: " + change.getType() + " " + topic.getId());
                bulkDocuments.add(topic.getData());
            }
            for (DocumentOperationResult dor : couchConnector.executeAllOrNothing(bulkDocuments)) {
                log.warn("Bulk update error (probably caused conflict): " + dor);
            }
        }       
    }

    // CouchDB single document CRUD operations

    void create(CouchTopic topic) {
        dataProvider.getCouchConnector().create(topic.getData());
        log.info("Created: " + topic.getId() + " " + topic.getName());
    }

    void update(CouchTopic topic) {
        dataProvider.getCouchConnector().update(topic.getData());        
        log.info("Updated: " + topic.getId() + " " + topic.getName());
    }

    boolean delete(CouchTopic topic) {
        log.info("Removing: " + topic.getId() + " " + topic.getName());
        try {
            dataProvider.getCouchConnector().delete(topic.getData());
            return true;
        } catch (UpdateConflictException e) {
            CouchTopic topic2 = (CouchTopic)dataProvider.getTopicById(topic.getId());
            if (topic2 != null) {
                dataProvider.getCouchConnector().delete(topic2.getData());
                return true;
            } else {
                return false;
            }
        }
    }

}
