package net.ontopia.presto.jaxrs.action;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;

public class SaveOrderedValuesAction extends FieldAction {

    @Override
    public boolean isActive(PrestoContextRules rules, PrestoField field, Projection projection, String actionId) {
        return true;
    }

    @Override
    public TopicView executeAction(PrestoContext context, TopicView topicView, PrestoField field, String actionId) {
        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();

        Presto session = getPresto();

        PrestoChangeSet changeSet = session.newChangeSet();
        PrestoUpdate update = changeSet.updateTopic(topic, type);

        update.setValues(field, getOrderedValues(topicView, field));

        changeSet.save();

        PrestoTopic topicAfterSave = update.getTopicAfterSave();
        PrestoContext newContext = PrestoContext.newContext(context, topicAfterSave);

        return session.getTopicViewAndProcess(newContext);
    }

    private Collection<?> getOrderedValues(TopicView topicView, PrestoField field) {
        Presto presto = getPresto();
        PrestoDataProvider dataProvider = presto.getDataProvider();
        boolean isReference = field.isReferenceField();
        List<Object> values = new ArrayList<Object>();
        for (FieldData fd : topicView.getFields()) {
            if (field.getId().equals(fd.getId())) {
                for (Value value : fd.getValues()) {
                    String v = value.getValue();
                    if (v != null) {
                        if (isReference) {
                            PrestoTopic vt = dataProvider.getTopicById(v);
                            if (vt != null) {
                                values.add(vt);
                            }
                        } else {
                            values.add(v);
                        }
                    }
                }
            }
        }
        return values;
    }

}
