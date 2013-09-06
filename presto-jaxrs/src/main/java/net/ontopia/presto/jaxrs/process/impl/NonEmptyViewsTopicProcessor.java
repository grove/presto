package net.ontopia.presto.jaxrs.process.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.PrestoContextRules;
import net.ontopia.presto.jaxrs.process.TopicProcessor;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public class NonEmptyViewsTopicProcessor extends TopicProcessor {

    @Override
    public Topic processTopic(Topic topicData, PrestoContextRules rules) {
        Set<String> viewsToHide = getViewsToHide(topicData, rules);
        if (!viewsToHide.isEmpty()) {
            hideEmptyViews(topicData, viewsToHide);
        }
        return topicData;
    }

    private Set<String> getViewsToHide(Topic topicData, PrestoContextRules rules) {
        ObjectNode config = getConfig();
        if (config != null) {
            JsonNode node = config.path("hideEmptyViews");
            if (node.isArray()) {
                return getViewsToHide((ArrayNode)node);
            }
        }
        return Collections.emptySet();
    }

    private Set<String> getViewsToHide(ArrayNode node) {
        Set<String> result = new HashSet<String>(node.size());
        for (JsonNode viewNode : node) {
            if (viewNode.isTextual()) {
                result.add(viewNode.getTextValue());
            }
        }
        return result;
    }

    private void hideEmptyViews(Topic topicData, Set<String> viewsToHide) {
        Iterator<TopicView> views = topicData.getViews().iterator();
        while (views.hasNext()) {
            TopicView view = views.next();
            if (viewsToHide.contains(view.getId()) && isEmptyView(view)) {
                views.remove();
            }
        }
    }

    private boolean isEmptyView(TopicView view) {
        if (TopicView.TOPIC_VIEW_REMOTE.equals(view.getFormat())) {
            return false;
        }
        Collection<FieldData> fields = view.getFields();
        if (fields != null) {
            for (FieldData field : fields) {
                if (!field.getValues().isEmpty()) {
                    return false;
                }
            }
        }
        return true;
    }

}
