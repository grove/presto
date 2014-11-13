package net.ontopia.presto.spi.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoChanges;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoDataProvider.ChangeSetHandler;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PrestoDefaultChangeSet implements PrestoChangeSet {

    private static Logger log = LoggerFactory.getLogger(PrestoDefaultChangeSet.class);

    public static interface DefaultDataProvider extends PrestoDataProvider {

        DefaultTopic newInstance(PrestoType type, String topicId);  // NOTE: if topic identity is null then assign one

        void create(PrestoTopic topic);

        void update(PrestoTopic topic);

        boolean delete(PrestoTopic topic);

        void updateBulk(List<Change> changes);

        Object deserializeFieldValue(PrestoField field, Object value);

        Object serializeFieldValue(PrestoField field, Object value);
        
    }

    public static interface DefaultTopic extends PrestoTopic {
        
        DefaultDataProvider getDataProvider();

        void updateNameProperty(Collection<? extends Object> values);

        void setValue(PrestoField field, Collection<? extends Object> values);

        void addValue(PrestoField field, Collection<? extends Object> values, int index);

        void removeValue(PrestoField field, Collection<? extends Object> values);

        void clearValue(PrestoField field);

    }

    static final int DEFAULT_INDEX = -1;

    public static interface Change {
        enum Type {CREATE, UPDATE, DELETE};
        Type getType();
        PrestoTopic getTopic();
        boolean isTopicUpdated();
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
        public boolean isTopicUpdated() {
            return true;
        }
    }

    private final DefaultDataProvider dataProvider;
    private final ChangeSetHandler handler;

    private final Set<PrestoDefaultUpdate> created = new HashSet<PrestoDefaultUpdate>();
    private final Map<PrestoTopic,PrestoDefaultUpdate> updates = new HashMap<PrestoTopic,PrestoDefaultUpdate>();
    private final Set<PrestoTopic> deleted = new HashSet<PrestoTopic>();

    private final List<Change> changes = new ArrayList<Change>();

    private boolean saved;

    public PrestoDefaultChangeSet(DefaultDataProvider dataProvider, ChangeSetHandler handler) {
        this.dataProvider = dataProvider;
        this.handler = handler;
    }

    public DefaultTopic newInstance(PrestoType type, String topicId) {
        return dataProvider.newInstance(type, topicId);
    }

    @Override
    public PrestoUpdate createTopic(PrestoType type) {
        PrestoDefaultUpdate update = new PrestoDefaultUpdate(this, type);
        changes.add(update);
        created.add(update);
        return update;
    }

    @Override
    public PrestoUpdate createTopic(PrestoType type, String topicId) {
        PrestoDefaultUpdate update = new PrestoDefaultUpdate(this, type, topicId);
        changes.add(update);
        created.add(update);
        return update;
    }

    @Override
    public PrestoUpdate updateTopic(PrestoTopic topic, PrestoType type) {
        PrestoDefaultUpdate update = updates.get(topic);
        if (update == null) {
            update = new PrestoDefaultUpdate(this, type, (DefaultTopic)topic);
            changes.add(update);
            updates.put(topic, update);
        }
        return update;
    }

    @Override
    public void deleteTopic(PrestoTopic topic, PrestoType type) {
        deleteTopic(topic, type, true, false);
    }

    @Override
    public void deleteTopic(PrestoTopic topic, PrestoType type, PrestoField field) {
        boolean isRemovableDependency = field.isCascadingDelete() && type.isRemovableCascadingDelete();
        deleteTopic(topic, type, true, isRemovableDependency);
    }

    private void deleteTopic(PrestoTopic topic, PrestoType type, boolean removeDependencies, boolean isRemovableDependency) {
        if (deleted.contains(topic)) {
            return;
        }

        if (!isRemovableDependency && !type.isRemovable()) {
            throw new RuntimeException("Cannot delete topic " + topic + " because type is not removable");
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
                removeInverseFieldValues(isNew, topic, field, topic.getValues(field));
            }
        }
    }

    private void removeDependencies(PrestoTopic topic, PrestoType type) {
        PrestoSchemaProvider schemaProvider = type.getSchemaProvider();
        for (PrestoTopic dependency : findRemovableDependencies(topic, type)) {
            if (!dependency.equals(topic)) {
                PrestoType dependencyType = schemaProvider.getTypeById(dependency.getTypeId());
                deleteTopic(dependency, dependencyType, false, true);
            }
        }
    }

    // dependent topics / cascading deletes

    private Collection<PrestoTopic> findRemovableDependencies(PrestoTopic topic, PrestoType type) {
        Collection<PrestoTopic> dependencies = new HashSet<PrestoTopic>();
        findRemovableDependencies(topic, type, dependencies);
        return dependencies;
    }

    private void findRemovableDependencies(PrestoTopic topic, PrestoType type, Collection<PrestoTopic> dependencies) {

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
                            findRemovableDependencies(valueTopic, valueType, dependencies);
                        }
                    } else {
                        log.warn("Value type with removableCascadingDelete=false in field with cascadingDelete=true.");
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

        if (handler != null) {
            handler.onBeforeSave(this);
        }
        
        this.saved = true;

        if (changes.size() == 1) {
            Change change = changes.get(0);
            if (change.isTopicUpdated()) {
                PrestoTopic topic = change.getTopic();
                if (topic.isInline()) {
                    throw new RuntimeException("Cannot save inline topic directly: " + topic);
                }
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
        
        if (handler != null) {
            handler.onAfterSave(this);
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
                    inverseUpdate.addValues(inverseField, Collections.singleton(topic), PrestoDefaultChangeSet.DEFAULT_INDEX);
                }
            }
        }
    }

    void removeInverseFieldValues(boolean isNew, PrestoTopic topic, PrestoField field, Collection<?> values) {
        if (!isNew) {
            String inverseFieldId = field.getInverseFieldId();
            if (inverseFieldId != null) {
                for (Object value : values) {

                    PrestoTopic valueTopic = (PrestoTopic)value;
                    if (!topic.equals(valueTopic)) {
                        PrestoType valueType = field.getSchemaProvider().getTypeById(valueTopic.getTypeId());
                        if (field.isCascadingDelete() && valueType.isRemovableCascadingDelete()) {
                            deleteTopic(valueTopic, valueType, field);
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

    @Override
    public PrestoChanges getPrestoChanges() {
        return new PrestoChanges() {
            @Override
            public Collection<? extends PrestoUpdate> getCreated() {
                // make copy as to prevent concurrent modification exceptions
                return new ArrayList<PrestoUpdate>(created);
            }

            @Override
            public Collection<? extends PrestoUpdate> getUpdated() {
                // make copy as to prevent concurrent modification exceptions
                return new ArrayList<PrestoUpdate>(updates.values());
            }

            @Override
            public Collection<? extends PrestoTopic> getDeleted() {
                // make copy as to prevent concurrent modification exceptions
                return new ArrayList<PrestoTopic>(deleted);
            }
        };
    }

}
