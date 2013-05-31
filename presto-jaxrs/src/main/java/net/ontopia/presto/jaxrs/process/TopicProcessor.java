package net.ontopia.presto.jaxrs.process;

import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxrs.PrestoContext;

public abstract class TopicProcessor extends AbstractProcessor {

    public abstract Topic processTopic(Topic topicData, PrestoContext context);

}
