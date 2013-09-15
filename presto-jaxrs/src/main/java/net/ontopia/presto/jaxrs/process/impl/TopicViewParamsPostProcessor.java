package net.ontopia.presto.jaxrs.process.impl;

import java.util.Map;

import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxrs.process.TopicViewProcessor;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;

import org.codehaus.jackson.node.ObjectNode;

public class TopicViewParamsPostProcessor extends TopicViewProcessor {

    @Override
    public TopicView processTopicView(TopicView topicView, PrestoContextRules rules) {
        PrestoContext context = rules.getContext();
        ObjectNode extraNode = getPresto().getViewExtraNode(context.getView());
        if (extraNode != null) {
            Map<String, Object> params = getPresto().getExtraParamsMap(extraNode);
            if (params != null) {
                topicView.setParams(params);
            }
        }
        return topicView;
    }

}
