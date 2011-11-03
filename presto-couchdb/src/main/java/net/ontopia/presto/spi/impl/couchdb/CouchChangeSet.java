package net.ontopia.presto.spi.impl.couchdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    private static Logger log = LoggerFactory.getLogger(CouchChangeSet.class.getName());

    static interface CouchChange {
        enum Type {CREATE, UPDATE, DELETE};
        Type getType();
        CouchTopic getTopic();
        boolean hasUpdate();
    }

    class CouchDelete implements CouchChange {
        private final CouchTopic topic;
        private CouchDelete(CouchTopic topic) {
            this.topic = topic;
        }
        @Override
        public Type getType() {
            return Type.DELETE;
        }
        @Override
        public CouchTopic getTopic() {
            return topic;
        }
        @Override
        public boolean hasUpdate() {
            return true;
        }
    }

    private final CouchDataProvider dataProvider;

    private final Set<PrestoTopic> deleted = new HashSet<PrestoTopic>();
    private final Map<PrestoTopic,CouchUpdate> updates = new HashMap<PrestoTopic,CouchUpdate>();
    private final List<CouchChange> changes = new ArrayList<CouchChange>();

    private boolean saved;

    CouchChangeSet(CouchDataProvider dataProvider) {
        this.dataProvider = dataProvider;
    }

    CouchTopic newInstance(PrestoType type) {
        return dataProvider.newInstance(type);
    }

    @Override
    public PrestoUpdate createTopic(PrestoType type) {
        CouchUpdate update = new CouchUpdate(this, type);
        changes.add(update);
        return update;
    }

    @Override
    public PrestoUpdate updateTopic(PrestoTopic topic, PrestoType type) {
        CouchUpdate update = updates.get(topic);
        if (update == null) {
            update = new CouchUpdate(this, (CouchTopic)topic, type);
            changes.add(update);
            updates.put(topic, update);
        }
        return update;
    }

    @Override
    public void deleteTopic(PrestoTopic topic, PrestoType type) {
        deleteTopic(topic, type, true);
    }

    private void deleteTopic(PrestoTopic topic, PrestoType type, boolean removeDependencies) {
        if (deleted.contains(topic)) {
            return;
        }

        changes.add(new CouchDelete((CouchTopic)topic));
        deleted.add(topic);
        
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

    @Override
    public void save() {
        if (saved) {
            log.warn("PrestoChangeSet.save() method called multiple times.");
            return; // idempotent
        }
        this.saved = true;
        
        if (changes.size() == 1) {
            CouchChange change = changes.get(0);
            if (change.hasUpdate()) {
                CouchTopic topic = change.getTopic();
                if (change.getType().equals(CouchChange.Type.CREATE)) {
                    create(topic);                
                } else if (change.getType().equals(CouchChange.Type.UPDATE)) {
                    if (!deleted.contains(topic)) {
                        update(topic);
                    }
                } else if (change.getType().equals(CouchChange.Type.DELETE)) {
                    delete(topic);                
                }
            }
        } else if (changes.size() > 1) {
            updateBulk(changes);
        }       
    }

    // CouchDB document CRUD operations

    protected void create(CouchTopic topic) {
        dataProvider.getCouchConnector().create(topic.getData());
        log.info("Created: " + topic.getId() + " " + topic.getName());
    }

    protected void update(CouchTopic topic) {
        dataProvider.getCouchConnector().update(topic.getData());        
        log.info("Updated: " + topic.getId() + " " + topic.getName());
    }

    protected void updateBulk(List<CouchChange> changes) {
        List<ObjectNode> bulkDocuments = new ArrayList<ObjectNode>();
        for (CouchChange change : changes) {
            if (change.hasUpdate()) {
                CouchTopic topic = change.getTopic();

                if (change.getType().equals(CouchChange.Type.DELETE)) {
                    ObjectNode data = topic.getData();
                    data.put("_deleted", true);
                }

                log.info("Bulk update document: " + change.getType() + " " + topic.getId());
                bulkDocuments.add(topic.getData());
            }
        }
        CouchDbConnector couchConnector = dataProvider.getCouchConnector();
        for (DocumentOperationResult dor : couchConnector.executeAllOrNothing(bulkDocuments)) {
            log.warn("Bulk update error (probably caused conflict): " + dor);
        }
    }

    protected boolean delete(CouchTopic topic) {
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

    // inverse fields (foreign keys)

    void addInverseFieldValue(boolean isNew, PrestoTopic topic, PrestoField field, Collection<?> values) {
        String inverseFieldId = field.getInverseFieldId();
        if (inverseFieldId != null) {
            for (Object value : values) {

                CouchTopic valueTopic = (CouchTopic)value;
                if (!topic.equals(valueTopic)) {
                    PrestoType valueType = field.getSchemaProvider().getTypeById(valueTopic.getTypeId());
                    PrestoField inverseField = valueType.getFieldById(inverseFieldId);
    
                    PrestoUpdate inverseUpdate = updateTopic(valueTopic, valueType);
                    inverseUpdate.addValues(inverseField, Collections.singleton(topic), CouchDataProvider.DEFAULT_INDEX);
                }
            }
        }
    }

    void removeInverseFieldValue(boolean isNew, PrestoTopic topic, PrestoField field, Collection<?> values) {
        if (!isNew) {
            String inverseFieldId = field.getInverseFieldId();
            if (inverseFieldId != null) {
                for (Object value : values) {

                    CouchTopic valueTopic = (CouchTopic)value;
                    if (!topic.equals(valueTopic)) {
                        PrestoType valueType = field.getSchemaProvider().getTypeById(valueTopic.getTypeId());
                        if (field.isCascadingDelete() && valueType.isRemovableCascadingDelete()) {
                            deleteTopic(valueTopic, valueType);
                        } else {          
                            PrestoField inverseField = valueType.getFieldById(inverseFieldId);
                            PrestoUpdate inverseUpdate = updateTopic(valueTopic, valueType);
                            inverseUpdate.removeValues(inverseField, Collections.singleton(topic));
                        }
                    }
                }
            }
        }
    }

}
