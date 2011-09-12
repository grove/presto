package net.ontopia.presto.spi.impl.couchdb;

import java.util.Collection;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;

public class CouchUpdate implements PrestoUpdate, CouchChangeSet.CouchChange {

    private static Logger log = LoggerFactory.getLogger(CouchUpdate.class.getName());

    private final CouchChangeSet changeSet;

    private CouchTopic topic;
    private final PrestoType type;
    private final boolean isNew;

    private int updateCount = 0;

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

    public boolean hasUpdate() {
        return isNew || updateCount > 0;
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

        if (!remValues.isEmpty() || !addValues.isEmpty()) {
            topic.setValue(field, values);
            handleFieldUpdated(field, addValues, remValues);
        }
    }

    public void addValues(PrestoField field, Collection<?> values) {
        addValues(field, values, CouchDataProvider.INDEX_DEFAULT);
    }

    public void addValues(PrestoField field, Collection<?> values, int index) {
        if (!values.isEmpty()) {
            topic.addValue(field, values, index);
            handleFieldUpdated(field, values, null);
        }
    }

    public void removeValues(PrestoField field, Collection<?> values) {
        if (!values.isEmpty()) {
            topic.removeValue(field, values);
            handleFieldUpdated(field, null, values);
        }
    }

    private void handleFieldUpdated(PrestoField field, Collection<?> addValues, Collection<?> remValues) {
        updateCount++;

        // update name property
        if (field.isNameField()) {
            topic.updateNameProperty(topic.getValues(field));
        }
        
        if (addValues != null && !addValues.isEmpty()) {
            log.info("+" + (isNew ? null : topic.getId()) + " " + field.getId() + " " + addValues);
            changeSet.addInverseFieldValue(isNew, topic, field, addValues);              
        }
        
        if (remValues != null && !remValues.isEmpty()) {
            log.info("-" + (isNew ? null : topic.getId()) + " " + field.getId() + " " + remValues);
            if (field.isReferenceField() && field.isCascadingDelete()) {
                // perform cascading delete
                for (Object value : remValues) {
                    PrestoTopic rtopic = (PrestoTopic)value;
                    PrestoType rtype = getSchemaProvider().getTypeById(rtopic.getTypeId());
                    if (rtype.isRemovableCascadingDelete()) {
                        changeSet.deleteTopic(rtopic, rtype);
                    }
                }
            } else {
                changeSet.removeInverseFieldValue(isNew, topic, field, remValues);
            }
        }        
    }
        
    private PrestoSchemaProvider getSchemaProvider() {
        return type.getSchemaProvider();
    }

}
