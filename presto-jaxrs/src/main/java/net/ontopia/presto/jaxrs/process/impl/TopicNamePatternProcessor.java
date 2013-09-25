package net.ontopia.presto.jaxrs.process.impl;

import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxrs.process.TopicProcessor;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PatternValueUtils;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoTopicWithParentFieldVariableResolver;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

public class TopicNamePatternProcessor extends TopicProcessor {

    @Override
    public Topic processTopic(Topic topicData, PrestoContextRules rules) {
        if (topicData.getName() == null) {
            PrestoContext context = rules.getContext();
            PrestoTopic topic = context.getTopic();
            PrestoVariableResolver variableResolver = new PrestoTopicWithParentFieldVariableResolver(getSchemaProvider(), context);
            String name = PatternValueUtils.getValueByPattern(variableResolver, topic, getConfig());
            if (name != null) {
                topicData.setName(name);
            }
        }
        return topicData;
    }

}
