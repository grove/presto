package net.ontopia.presto.jaxrs.process.impl;

import java.util.Map;

import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxrs.process.TopicViewProcessor;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;

import org.codehaus.jackson.node.ObjectNode;

public class TopicViewParamsPostProcessor extends TopicViewProcessor {

    @Override
    public TopicView processTopicView(TopicView topicView, PrestoTopic topic, PrestoType type, PrestoView view) {
        ObjectNode extraNode = getPresto().getViewExtraNode(view);
        if (extraNode != null) {
            Map<String, Object> params = getPresto().getExtraParamsMap(extraNode);
            if (params != null) {
                topicView.setParams(params);
            }
        }
        return topicView;
    }

}
