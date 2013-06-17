package net.ontopia.presto.jaxrs.process.impl;

import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.process.TopicProcessor;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoTopicFieldVariableResolver;

public class TopicNamePatternProcessor extends TopicProcessor {

    @Override
    public Topic processTopic(Topic topicData, PrestoContext context) {
        if (topicData.getName() == null) {
            PrestoTopic topic = context.getTopic();
            PrestoTopicFieldVariableResolver variableResolver = new PrestoTopicFieldVariableResolver(getSchemaProvider());
            String name = PatternValueUtils.getValueByPattern(variableResolver, topic, getConfig());
            if (name != null) {
                topicData.setName(name);
            }
        }
        return topicData;
    }

}
