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

public class ClearValuesFieldAction implements FieldAction {

    @Override
    public TopicView executeAction(Presto session, PrestoContext context, TopicView topicView, PrestoFieldUsage field, String actionId) {
        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();
        
        PrestoChangeSet changeSet = session.newChangeSet();
        PrestoUpdate update = changeSet.updateTopic(topic, type);        
        
        update.clearValues(field);
        
        changeSet.save();
        
        PrestoTopic topicAfterSave = update.getTopicAfterSave();
        FieldData fieldData = session.getFieldData(topicAfterSave, field);
        
        replaceFieldData(topicView, fieldData);
        
        return topicView;
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
