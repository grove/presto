package net.ontopia.presto.jaxrs.process;

import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;

public abstract class TopicProcessor extends AbstractProcessor {

    public abstract Topic processTopic(Topic topicData, PrestoTopic topic, PrestoType type, PrestoView view);

}
