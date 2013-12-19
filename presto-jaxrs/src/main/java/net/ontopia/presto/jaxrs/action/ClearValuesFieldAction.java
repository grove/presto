package net.ontopia.presto.jaxrs.action;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.Utils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public abstract class ClearValuesFieldAction extends FieldAction {

    @Override
    public TopicView executeAction(PrestoContext context, TopicView topicView, PrestoFieldUsage field, String actionId) {
        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();
        
        Presto session = getPresto();
                
        PrestoChangeSet changeSet = session.newChangeSet();
        PrestoUpdate update = changeSet.updateTopic(topic, type);        
        
        update.clearValues(field);
        
        changeSet.save();
        
        PrestoTopic topicAfterSave = update.getTopicAfterSave();
        PrestoContext newContext = PrestoContext.newContext(context, topicAfterSave);
        
        refreshFields(topicView, field, session, newContext);
        
        return topicView;
    }

    protected Collection<String> getRefreshFieldIds(TopicView topicView, PrestoFieldUsage field, Presto session, PrestoContext newContext) {
        List<String> result = new ArrayList<String>();
        result.add(field.getId());
        ObjectNode config  = getConfig();
        if (config != null && config.isObject()) {
            for (JsonNode refreshField : config.path("refresh-fields")) {
                String refreshFieldId = refreshField.getTextValue();
                if (refreshFieldId != null) {
                    result.add(refreshFieldId);
                }
            }
        }
        return result;
    }
    
    private void refreshFields(TopicView topicView, PrestoFieldUsage field, Presto session, PrestoContext newContext) {
        for (String fieldId : getRefreshFieldIds(topicView, field, session, newContext)) {
            refreshField(session, topicView, newContext, fieldId);
        }
    }

    private void refreshField(Presto session, TopicView topicView, PrestoContext context, String fieldId) {
        PrestoFieldUsage field = context.getFieldById(fieldId);
        FieldData fieldData = session.getFieldData(context.getTopic(), field);
        replaceFieldData(topicView, fieldData);
    }

    private void replaceFieldData(TopicView topicView, FieldData fieldData) {
        String fieldId = fieldData.getId();
        Collection<FieldData> fields = topicView.getFields();
        List<FieldData> result = new ArrayList<FieldData>(fields.size());
        for (FieldData fd : fields) {
            if (Utils.equals(fieldId, fd.getId())) {
                result.add(fieldData);
            } else {
                result.add(fd);
            }
        }
        topicView.setFields(result);
    }

}
