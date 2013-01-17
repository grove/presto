package net.ontopia.presto.jaxrs.process.impl;

import java.util.Map;

import org.codehaus.jackson.node.ObjectNode;

import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxrs.process.TopicProcessor;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;

public class TopicParamsPostProcessor extends TopicProcessor {

    @Override
    public Topic processTopic(Topic topicData, PrestoTopic topic, PrestoType type, PrestoView view) {
        ObjectNode extraNode = getPresto().getTypeExtraNode(type);
        if (extraNode != null) {
            Map<String, Object> params = getPresto().getExtraParamsMap(extraNode);
            if (params != null) {
                topicData.setParams(params);
            }
        }
        return topicData;
    }

}
