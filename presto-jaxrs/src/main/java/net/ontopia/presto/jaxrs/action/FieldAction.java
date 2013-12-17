package net.ontopia.presto.jaxrs.action;

import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.utils.PrestoContext;

public interface FieldAction {

    TopicView executeAction(Presto session, PrestoContext context, TopicView topicView, PrestoFieldUsage field, String actionId);
    
}
