package net.ontopia.presto.jaxrs.action;

import java.util.HashSet;
import java.util.Set;

import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxb.utils.TopicViewUtils;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;
import net.ontopia.presto.spi.utils.PrestoContext;

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
        
        
        TopicView newTopicView = session.getTopicViewAndProcess(newContext);
        
        Set<String> ignoreFieldIds = getRefreshFieldIds(topicView, field, session, newContext);
        TopicViewUtils.copyFieldValues(newTopicView, topicView, ignoreFieldIds);
        
        return newTopicView;
    }
    
    protected Set<String> getRefreshFieldIds(TopicView topicView, PrestoFieldUsage field, Presto session, PrestoContext newContext) {
        Set<String> result = new HashSet<String>();
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

}
