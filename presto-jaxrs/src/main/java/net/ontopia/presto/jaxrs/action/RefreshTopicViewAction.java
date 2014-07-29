package net.ontopia.presto.jaxrs.action;

import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;

public class RefreshTopicViewAction extends FieldAction {

    @Override
    public boolean isActive(PrestoContextRules rules, PrestoField field, Projection projection, String actionId) {
        return true;
//        if (projection != null) {
//            String orderBy = projection.getOrderBy();
//            return orderBy != null;
//        }
//        return false;
    }

    @Override
    public TopicView executeAction(PrestoContext context, TopicView topicView, PrestoField field, String actionId) {
        Presto session = getPresto();
        return session.getTopicViewAndProcess(context);
    }

}
