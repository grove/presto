package net.ontopia.presto.spi.utils;

import java.util.Collection;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;
import net.ontopia.presto.spi.utils.PrestoDefaultChangeSet.DefaultTopic;

public class PrestoDefaultUpdate implements PrestoUpdate, PrestoDefaultChangeSet.Change {

    private static Logger log = LoggerFactory.getLogger(PrestoDefaultUpdate.class.getName());

    private final PrestoDefaultChangeSet changeSet;

    private final DefaultTopic topic;
    private final PrestoType type;
    private final boolean isNew;

    private int updateCount = 0;

    PrestoDefaultUpdate(PrestoDefaultChangeSet changeSet, PrestoType type) {
        this(changeSet, null, type);
    }

    PrestoDefaultUpdate(PrestoDefaultChangeSet changeSet, DefaultTopic topic, PrestoType type) {
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
    
    @Override
    public Type getType() {
        return isNew ? Type.CREATE : Type.UPDATE;
    }
    
    @Override
    public PrestoTopic getTopic() {
        return topic;
    }
    
    @Override
    public PrestoTopic getTopicAfterUpdate() {
        return topic.getDataProvider().getTopicById(topic.getId());
    }

    @Override
    public boolean hasUpdate() {
        return isNew || updateCount > 0;
    }

    @Override
    public void setValues(PrestoField field, Collection<?> values) {
        Collection<? extends Object> existingValues = topic.getValues(field);
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

    @Override
    public void addValues(PrestoField field, Collection<?> values) {
        addValues(field, values, PrestoDefaultChangeSet.DEFAULT_INDEX);
    }

    @Override
    public void addValues(PrestoField field, Collection<?> values, int index) {
        if (!values.isEmpty()) {
            topic.addValue(field, values, index);
            handleFieldUpdated(field, values, null);
        }
    }

    @Override
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
