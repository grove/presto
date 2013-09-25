package net.ontopia.presto.jaxrs.process;

import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.spi.utils.PrestoContextRules;

public abstract class TopicProcessor extends AbstractProcessor {

    public abstract Topic processTopic(Topic topicData, PrestoContextRules rules);

}
