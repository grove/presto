package net.ontopia.presto.spi.impl.couchdb;

import java.util.Collection;
import java.util.HashSet;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;

public class CouchUpdate implements PrestoUpdate, CouchChangeSet.CouchChange {

    private final CouchChangeSet changeSet;

    private CouchTopic topic;
    private final PrestoType type;
    private final boolean isNew;

    CouchUpdate(CouchChangeSet changeSet, PrestoType type) {
        this(changeSet, null, type);
    }

    CouchUpdate(CouchChangeSet changeSet, CouchTopic topic, PrestoType type) {
        if (topic == null && type == null) {
            throw new IllegalArgumentException("At least one of topic or type must be specified.");
        }
        this.changeSet = changeSet;
        this.type = type;
        this.isNew = (topic == null);
        
        if (topic == null) {
            if (type != null) {
                this.topic = changeSet.newInstance(type);
            } else {
                throw new RuntimeException("No topic and no type. I'm sorry, Dave. I'm afraid I can't do that.");
            }
        } else {
            this.topic = topic;
        }

    }
    
    public Type getType() {
        return isNew ? Type.CREATE : Type.UPDATE;
    }
    
    public CouchTopic getTopic() {
        return topic;
    }
    
    public PrestoTopic getTopicAfterUpdate() {
        return topic.getDataProvider().getTopicById(topic.getId());
    }
    
    public void setValues(PrestoField field, Collection<?> values) {
        Collection<Object> existingValues = topic.getValues(field);
        Collection<Object> remValues = new HashSet<Object>(existingValues);          
        Collection<Object> addValues = new HashSet<Object>();

        for (Object value : values) {
            remValues.remove(value);
            if (!existingValues.contains(value)) {
                addValues.add(value);
            }
        }

        topic.setValue(field, values);
        handleFieldAddValues(field, addValues);
        handleFieldRemoveValues(field, remValues);
        handleFieldUpdated(field);
    }

    public void addValues(PrestoField field, Collection<?> values) {
        addValues(field, values, CouchDataProvider.INDEX_DEFAULT);
    }

    public void addValues(PrestoField field, Collection<?> values, int index) {
        topic.addValue(field, values, index);
        handleFieldAddValues(field, values);
        handleFieldUpdated(field);
    }

    public void removeValues(PrestoField field, Collection<?> values) {
        topic.removeValue(field, values);
        handleFieldRemoveValues(field, values);
        handleFieldUpdated(field);
    }

    private void handleFieldUpdated(PrestoField field) {
        // update name property
        if (field.isNameField()) {
            topic.updateNameProperty(topic.getValues(field));
        }
    }
    
    private void handleFieldAddValues(PrestoField field, Collection<?> values) {
//        changeSet.addInverseFieldValue(isNew, topic, field, values);              
    }

    private void handleFieldRemoveValues(PrestoField field, Collection<?> values) {
        if (field.isReferenceField() && field.isCascadingDelete()) {
            // perform cascading delete
            for (Object value : values) {
                PrestoTopic rtopic = (PrestoTopic)value;
                PrestoType rtype = getSchemaProvider().getTypeById(rtopic.getTypeId());
                if (rtype.isRemovableCascadingDelete()) {
                    changeSet.deleteTopic(rtopic, rtype);
                }
            }
//        } else {
//            changeSet.removeInverseFieldValue(isNew, topic, field, entry.getValue());
        }
        
    }
    
    
    private PrestoSchemaProvider getSchemaProvider() {
        return type.getSchemaProvider();
    }

}
