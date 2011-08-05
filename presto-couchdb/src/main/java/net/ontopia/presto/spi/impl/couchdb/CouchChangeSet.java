package net.ontopia.presto.spi.impl.couchdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

public class CouchChangeSet implements PrestoChangeSet {

    private final CouchDataProvider dataProvider;

    private CouchTopic topic;
    private final PrestoType type;

    private final List<Change> changes = new ArrayList<Change>();
    private boolean saved;

    CouchChangeSet(CouchDataProvider dataProvider, PrestoType type) {
        this.dataProvider = dataProvider;
        this.topic = null;
        this.type = type;
    }

    CouchChangeSet(CouchDataProvider dataProvider, CouchTopic topic, PrestoType type) {
        this.dataProvider = dataProvider;
        this.topic = topic;
        this.type = type;
    }

    public void setValues(PrestoField field, Collection<?> values) {
        if (saved) {
            throw new RuntimeException("Can only save a changeset once.");
        }
        changes.add(new Change(Change.ChangeType.SET, field, values));
    }

    public void addValues(PrestoField field, Collection<?> values) {
        addValues(field, values, Change.INDEX_DEFAULT);
    }

    public void addValues(PrestoField field, Collection<?> values, int index) {
        if (saved) {
            throw new RuntimeException("Can only save a changeset once.");
        }
        changes.add(new Change(Change.ChangeType.ADD, field, values, index));
    }

    public void removeValues(PrestoField field, Collection<?> values) {
        if (saved) {
            throw new RuntimeException("Can only save a changeset once.");
        }
        changes.add(new Change(Change.ChangeType.REMOVE, field, values));
    }

    public PrestoTopic save() {
        this.saved = true;

        boolean isNew = false;
        if (topic == null) {
            if (type != null) {
                topic = dataProvider.newInstance(type);
                isNew = true;
            } else {
                throw new RuntimeException("No topic and no type. I'm sorry, Dave. I'm afraid I can't do that.");
            }
        }

        Map<PrestoField, Collection<Object>> addFieldValues = new LinkedHashMap<PrestoField, Collection<Object>>();
        Map<PrestoField, Collection<Object>> remFieldValues = new LinkedHashMap<PrestoField, Collection<Object>>();

        for (Change change : changes) {
            PrestoField field = change.getField();
            Collection<?> values = change.getValues();
            switch(change.getType()) {
            case SET: {
                Collection<Object> existingValues = topic.getValues(field);
                Collection<Object> remValues = new HashSet<Object>(existingValues);          
                Collection<Object> addValues = new HashSet<Object>();

                for (Object value : values) {
                    remValues.remove(value);
                    if (!existingValues.contains(value)) {
                        addValues.add(value);
                    }
                }

                registerValues(field, addFieldValues, addValues);
                registerValues(field, remFieldValues, remValues);

                topic.setValue(field, values);
                break;
            }
            case ADD: {
                registerValues(field, addFieldValues, values);
                topic.addValue(field, values, change.getIndex());
                break;
            }
            case REMOVE: {
                registerValues(field, remFieldValues, values);
                topic.removeValue(field, values);
                break;
            }
            }

            // update name property
            if (field.isNameField()) {
                topic.updateNameProperty(values);
            }
        }
        if (isNew) {
            dataProvider.create(topic);
        } else {
            dataProvider.update(topic);      
        }

        for (Map.Entry<PrestoField,Collection<Object>> entry : addFieldValues.entrySet()) {
            dataProvider.addInverseFieldValue(isNew, topic, entry.getKey(), entry.getValue());      
        }
        for (Map.Entry<PrestoField,Collection<Object>> entry : remFieldValues.entrySet()) {
            PrestoField field = entry.getKey();
            Collection<Object> values = entry.getValue();
            if (field.isReferenceField() && field.isCascadingDelete()) {
                // perform cascading delete
                for (Object value : values) {
                    PrestoTopic rtopic = (PrestoTopic)value;
                    PrestoType rtype = getSchemaProvider().getTypeById(rtopic.getTypeId());
                    dataProvider.deleteTopic(rtopic, rtype);
                }
            } else {
                dataProvider.removeInverseFieldValue(isNew, topic, field, entry.getValue());
            }
        }

        return topic;
    }

    private void registerValues(PrestoField field, Map<PrestoField, Collection<Object>> fieldValues, Collection<?> values) {
        Collection<Object> coll = fieldValues.get(field);
        if (coll == null) {
            coll = new HashSet<Object>();
            fieldValues.put(field, coll);
        }
        coll.addAll(values);
    }

    private PrestoSchemaProvider getSchemaProvider() {
        return type.getSchemaProvider();
    }

    private static class Change {

        static enum ChangeType { SET, ADD, REMOVE };

        static final int INDEX_DEFAULT = -1;

        private ChangeType type;
        private final PrestoField field;
        private final Collection<?> values;
        private final int index;

        Change(ChangeType type, PrestoField field, Collection<?> values) {
            this(type, field, values, INDEX_DEFAULT);
        }

        Change(ChangeType type, PrestoField field, Collection<?> values, int index) {
            this.type = type;
            this.field = field;
            this.values = values;      
            this.index = index;
        }

        public ChangeType getType() {
            return type;
        }

        public PrestoField getField() {
            return field;
        }

        public Collection<?> getValues() {
            return values;
        }
        public int getIndex() {
            return index;
        }
    }

}
