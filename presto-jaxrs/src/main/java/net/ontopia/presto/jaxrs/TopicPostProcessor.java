package net.ontopia.presto.jaxrs;

import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;

public abstract class TopicPostProcessor extends AbstractProcessor {

    public abstract Topic postProcess(Topic topicData, PrestoTopic topic, PrestoType type, PrestoView view);

}
