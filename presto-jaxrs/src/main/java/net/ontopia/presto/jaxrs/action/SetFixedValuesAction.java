package net.ontopia.presto.jaxrs.action;

import java.util.ArrayList;
import java.util.List;

import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class SetFixedValuesAction extends FieldAction {

    @Override
    public boolean isActive(PrestoContextRules rules, PrestoField field, String actionId) {
        return true;
    }

    @Override
    public TopicView executeAction(PrestoContext context, TopicView topicView, PrestoField field, String actionId) {
        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();

        Presto session = getPresto();

        PrestoChangeSet changeSet = session.newChangeSet();
        PrestoUpdate update = changeSet.updateTopic(topic, type);

        update.setValues(field, getFixedValues());

        changeSet.save();

        PrestoTopic topicAfterSave = update.getTopicAfterSave();
        PrestoContext newContext = PrestoContext.newContext(context, topicAfterSave);

        return session.getTopicViewAndProcess(newContext);
    }

    private List<? extends Object> getFixedValues() {
        List<Object> values = new ArrayList<Object>();

        ObjectNode config = getConfig();
        JsonNode valuesNode = config.path("values");
        if (valuesNode.isArray()) {
            for (JsonNode valueNode : valuesNode) {
                if (valueNode.isTextual()) {
                    String value = valueNode.getTextValue();
                    if (value != null) {
                        values.add(value);
                    }
                }
            }
        }
        return values;
    }

}
