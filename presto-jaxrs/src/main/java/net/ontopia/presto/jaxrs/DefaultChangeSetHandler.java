package net.ontopia.presto.jaxrs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoChanges;
import net.ontopia.presto.spi.PrestoDataProvider.ChangeSetHandler;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public abstract class DefaultChangeSetHandler implements ChangeSetHandler {

    protected abstract PrestoSchemaProvider getSchemaProvider();
    
    protected abstract Collection<String> getVariableValues(PrestoTopic topic, PrestoType type, PrestoField field, String variable);

    @Override
    public void onBeforeSave(PrestoChangeSet changeSet, PrestoChanges changes) {
        for (PrestoUpdate update : changes.getUpdates()) {
            if (update.isTopicUpdated()) {
                assignDefaultValues(update);
            }
        }
    }

    @Override
    public void onAfterSave(PrestoChangeSet changeSet, PrestoChanges changes) {
    }

    protected void assignDefaultValues(PrestoUpdate update) {
        PrestoTopic topic = update.getTopic();
        PrestoSchemaProvider schemaProvider = getSchemaProvider();
        PrestoType type = schemaProvider.getTypeById(topic.getTypeId());
        boolean isNewTopic = update.isNewTopic();

        for (PrestoField field : type.getFields()) {
            ObjectNode extra = (ObjectNode)field.getExtra();
            if (extra != null) {
                JsonNode assignment = extra.path("assignment");
                if (assignment.isObject()) {
                    List<Object> defaultValues = null;

                    String valuesAssignmentType = assignment.get("type").getTextValue();
                    if (valuesAssignmentType.equals("create")) {
                        if (isNewTopic) {
                            defaultValues = getDefaultValues(topic, type, field, assignment);                    
                        }
                    } else if (valuesAssignmentType.equals("update")) {
                        JsonNode fields = assignment.path("fields");
                        if (fields.isArray()) {
                            // update only if any of the given fields are updated 
                            for (JsonNode fieldNode : fields) {
                                String fieldId = fieldNode.getTextValue();
                                PrestoField fieldById = type.getFieldById(fieldId);
                                if (update.isFieldUpdated(fieldById)) {
                                    defaultValues = getDefaultValues(topic, type, field, assignment);
                                }
                            }
                        } else {
                            defaultValues = getDefaultValues(topic, type, field, assignment);
                        }
                    }
                    if (defaultValues != null) {
                        update.setValues(field, defaultValues);                
                    }
                }
            }
        }
    }

    protected List<Object> getInitialValues(PrestoType type, PrestoField field) {
        ObjectNode extra = (ObjectNode)field.getExtra();
        JsonNode assignment = extra.path("assigment");
        if (assignment.isObject()) {
            String valuesAssignmentType = assignment.get("type").getTextValue();
            if (valuesAssignmentType.equals("initial")) {
                return getDefaultValues(null, type, field, assignment);                    
            }
        }
        return Collections.emptyList();
    }

    protected List<Object> getDefaultValues(PrestoTopic topic, PrestoType type, PrestoField field, JsonNode assignment) {
        List<Object> result = new ArrayList<Object>();
        for (JsonNode valueNode : assignment.get("values")) {
            String value = valueNode.getTextValue();
            if (value != null) {
                if (value.charAt(0) == '$') {
                    String variable = value.substring(1);
                    for (String varValue : getVariableValues(topic, type, field, variable)) {
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