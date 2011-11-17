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
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultChangeSet implements PrestoChangeSet {

    private static Logger log = LoggerFactory.getLogger(DefaultChangeSet.class.getName());
    
    public static interface DefaultDataProvider extends PrestoDataProvider {

        DefaultTopic newInstance(PrestoType type);

        void create(PrestoTopic topic);

        void update(PrestoTopic topic);

        boolean delete(PrestoTopic topic);

        void updateBulk(List<Change> changes);

    }
    
    public static interface DefaultTopic extends PrestoTopic {

        PrestoDataProvider getDataProvider();
        
        void updateNameProperty(Collection<? extends Object> values);

        void setValue(PrestoField field, Collection<? extends Object> values);

        void addValue(PrestoField field, Collection<? extends Object> values, int index);

        void removeValue(PrestoField field, Collection<? extends Object> values);

    }
    static final int DEFAULT_INDEX = -1;

    static interface Change {
        enum Type {CREATE, UPDATE, DELETE};
        Type getType();
        PrestoTopic getTopic();
        boolean hasUpdate();
    }

    class ChangeDelete implements Change {
        private final PrestoTopic topic;
        private ChangeDelete(PrestoTopic topic) {
            this.topic = topic;
        }
        @Override
        public Type getType() {
            return Type.DELETE;
        }
        @Override
        public PrestoTopic getTopic() {
            return topic;
        }
        @Override
        public boolean hasUpdate() {
            return true;
        }
    }

    private final DefaultDataProvider dataProvider;

    private final Set<PrestoTopic> deleted = new HashSet<PrestoTopic>();
    private final Map<PrestoTopic,DefaultUpdate> updates = new HashMap<PrestoTopic,DefaultUpdate>();
    private final List<Change> changes = new ArrayList<Change>();

    private boolean saved;

    DefaultChangeSet(DefaultDataProvider dataProvider) {
        this.dataProvider = dataProvider;
    }

    DefaultTopic newInstance(PrestoType type) {
        return dataProvider.newInstance(type);
    }

    @Override
    public PrestoUpdate createTopic(PrestoType type) {
        DefaultUpdate update = new DefaultUpdate(this, type);
        changes.add(update);
        return update;
    }

    @Override
    public PrestoUpdate updateTopic(PrestoTopic topic, PrestoType type) {
        DefaultUpdate update = updates.get(topic);
        if (update == null) {
            update = new DefaultUpdate(this, (DefaultTopic)topic, type);
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

        changes.add(new ChangeDelete(topic));
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
            Change change = changes.get(0);
            if (change.hasUpdate()) {
                PrestoTopic topic = change.getTopic();
                if (change.getType().equals(Change.Type.CREATE)) {
                    dataProvider.create(topic);                
                } else if (change.getType().equals(Change.Type.UPDATE)) {
                    if (!deleted.contains(topic)) {
                        dataProvider.update(topic);
                    }
                } else if (change.getType().equals(Change.Type.DELETE)) {
                    dataProvider.delete(topic);                
                }
            }
        } else if (changes.size() > 1) {
            dataProvider.updateBulk(changes);
        }       
    }

    // inverse fields (foreign keys)

    void addInverseFieldValue(boolean isNew, PrestoTopic topic, PrestoField field, Collection<?> values) {
        String inverseFieldId = field.getInverseFieldId();
        if (inverseFieldId != null) {
            for (Object value : values) {

                PrestoTopic valueTopic = (PrestoTopic)value;
                if (!topic.equals(valueTopic)) {
                    PrestoType valueType = field.getSchemaProvider().getTypeById(valueTopic.getTypeId());
                    PrestoField inverseField = valueType.getFieldById(inverseFieldId);
    
                    PrestoUpdate inverseUpdate = updateTopic(valueTopic, valueType);
                    inverseUpdate.addValues(inverseField, Collections.singleton(topic), DefaultChangeSet.DEFAULT_INDEX);
                }
            }
        }
    }

    void removeInverseFieldValue(boolean isNew, PrestoTopic topic, PrestoField field, Collection<?> values) {
        if (!isNew) {
            String inverseFieldId = field.getInverseFieldId();
            if (inverseFieldId != null) {
                for (Object value : values) {

                    PrestoTopic valueTopic = (PrestoTopic)value;
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
