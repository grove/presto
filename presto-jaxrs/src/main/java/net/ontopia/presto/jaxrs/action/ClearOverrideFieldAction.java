package net.ontopia.presto.jaxrs.action;

import java.util.Collection;
import java.util.List;

import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;

import org.codehaus.jackson.node.ObjectNode;

public class ClearOverrideFieldAction extends ClearValuesFieldAction {

    @Override
    public boolean isActive(PrestoContextRules rules, PrestoFieldUsage field, String actionId) {
        return isOverrideFieldNotEmpty(rules);
    }

    private boolean isOverrideFieldNotEmpty(PrestoContextRules rules) {
        String overrideFieldId = getOverrideFieldId();
        PrestoContext context = rules.getContext();
        PrestoFieldUsage overrideField = context.getFieldById(overrideFieldId);
        PrestoTopic topic = context.getTopic();
        List<? extends Object> values = topic.getValues(overrideField);
        return !values.isEmpty();
    }

    @Override
    protected Collection<String> getRefreshFieldIds(TopicView topicView, PrestoFieldUsage field, Presto session, PrestoContext newContext) {
        Collection<String> result = super.getRefreshFieldIds(topicView, field, session, newContext);
        String overrideFieldId = getOverrideFieldId();
        if (overrideFieldId != null) {
            result.add(overrideFieldId);
        }
        return result;
    }
    
    protected String getOverrideFieldId() {
        ObjectNode config  = getConfig();
        if (config != null && config.isObject()) {
            String result = config.path("override-field").getTextValue();
            if (result != null) {
                return result;
            }
        }
        throw new RuntimeException("'override-field' missing from field action config" + config);
    }
    
}
