package net.ontopia.presto.jaxrs.process.impl;

import java.util.Map;

import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxrs.process.TopicProcessor;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;

import org.codehaus.jackson.node.ObjectNode;

public class TopicParamsPostProcessor extends TopicProcessor {

    @Override
    public Topic processTopic(Topic topicData, PrestoContextRules rules) {
        PrestoContext context = rules.getContext();
        ObjectNode extraNode = getPresto().getTypeExtraNode(context.getType());
        if (extraNode != null) {
            Map<String, Object> params = getPresto().getExtraParamsMap(extraNode);
            if (params != null) {
                topicData.setParams(params);
            }
        }
        return topicData;
    }

}
