package net.ontopia.presto.jaxrs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoChanges;
import net.ontopia.presto.spi.PrestoDataProvider.ChangeSetHandler;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class DefaultChangeSetHandler implements ChangeSetHandler {

    protected abstract PrestoSchemaProvider getSchemaProvider();
    
    protected abstract Collection<? extends Object> getVariableValues(PrestoTopic topic, PrestoType type, PrestoField field, String variable);

    @Override
    public void onBeforeSave(PrestoChangeSet changeSet) {
        PrestoChanges changes = changeSet.getPrestoChanges();
        for (PrestoUpdate update : changes.getCreated()) {
            assignDefaultValues(update);
        }
        for (PrestoUpdate update : changes.getUpdated()) {
            if (update.isTopicUpdated()) {
                assignDefaultValues(update);
            }
        }
    }

    @Override
    public void onAfterSave(PrestoChangeSet changeSet) {
    }

    protected void assignDefaultValues(PrestoUpdate update) {
        PrestoTopic topic = update.getTopic();
        PrestoSchemaProvider schemaProvider = getSchemaProvider();
        PrestoType type = schemaProvider.getTypeById(topic.getTypeId());

        for (PrestoField field : type.getFields()) {
            assignDefaultValues(update, topic, type, field);
        }
    }

    private void assignDefaultValues(PrestoUpdate update, PrestoTopic topic,
            PrestoType type, PrestoField field) {
        ObjectNode extra = (ObjectNode)field.getExtra();
        if (extra != null) {
            JsonNode assignment = extra.path("assignment");
            if (assignment.isObject()) {
                List<Object> defaultValues = null;

                String valuesAssignmentType = assignment.get("type").textValue();

                if (valuesAssignmentType.equals("create")) {
                    if (update.isNewTopic()) {
                        defaultValues = getDefaultValues(topic, type, field, assignment);                    
                    }
                
                } else if (valuesAssignmentType.equals("update")) {
                    JsonNode fields = assignment.path("fields");
                    if (fields.isArray()) {
                        // update only if any of the given fields are updated 
                        for (JsonNode fieldNode : fields) {
                            String fieldId = fieldNode.textValue();
                            PrestoField fieldById = type.getFieldById(fieldId);
                            if (update.isFieldUpdated(fieldById)) {
                                defaultValues = getDefaultValues(topic, type, field, assignment);
                            }
                        }
                    } else {
                        defaultValues = getDefaultValues(topic, type, field, assignment);
                    }

                } else if (valuesAssignmentType.equals("first-update")) {
                    if (topic.getValues(field).isEmpty()) {
                        JsonNode fields = assignment.path("fields");
                        if (fields.isArray()) {
                            // update only if any of the given fields are updated 
                            for (JsonNode fieldNode : fields) {
                                String fieldId = fieldNode.textValue();
                                PrestoField fieldById = type.getFieldById(fieldId);
                                if (update.isFieldUpdated(fieldById)) {
                                    defaultValues = getDefaultValues(topic, type, field, assignment);
                                }
                            }
                        } else {
                            defaultValues = getDefaultValues(topic, type, field, assignment);
                        }
                    }
                }
                if (defaultValues != null) {
                    update.setValues(field, defaultValues);                
                }
            }
        }
    }

//    protected List<Object> getInitialValues(PrestoType type, PrestoField field) {
//        ObjectNode extra = (ObjectNode)field.getExtra();
//        JsonNode assignment = extra.path("assigment");
//        if (assignment.isObject()) {
//            String valuesAssignmentType = assignment.get("type").textValue();
//            if (valuesAssignmentType.equals("initial")) {
//                return getDefaultValues(null, type, field, assignment);                    
//            }
//        }
//        return Collections.emptyList();
//    }

    protected List<Object> getDefaultValues(PrestoTopic topic, PrestoType type, PrestoField field, JsonNode assignment) {
        List<Object> result = new ArrayList<Object>();
        for (JsonNode valueNode : assignment.get("values")) {
            String value = valueNode.textValue();
            if (value != null) {
                if (value.charAt(0) == '$') {
                    String variable = value.substring(1);
                    for (Object varValue : getVariableValues(topic, type, field, variable)) {
                        if (varValue != null) {
                            result.add(varValue);
                        }
                    }
                } else {
                    result.add(value);
                }
            }
        }
        return result;
    }

}