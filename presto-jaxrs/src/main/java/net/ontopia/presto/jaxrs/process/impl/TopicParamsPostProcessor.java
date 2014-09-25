package net.ontopia.presto.jaxrs.process.impl;

import java.util.Map;

import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxrs.process.TopicProcessor;
import net.ontopia.presto.spi.utils.ExtraUtils;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class TopicParamsPostProcessor extends TopicProcessor {

    @Override
    public Topic processTopic(Topic topicData, PrestoContextRules rules) {
        PrestoContext context = rules.getContext();
        ObjectNode extraNode = ExtraUtils.getTypeExtraNode(context.getType());
        if (extraNode != null) {
            Map<String, Object> params = ExtraUtils.getExtraParamsMap(extraNode);
            if (params != null) {
                topicData.setParams(params);
            }
        }
        return topicData;
    }

}
