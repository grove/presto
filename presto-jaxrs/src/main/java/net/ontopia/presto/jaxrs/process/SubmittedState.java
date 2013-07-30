package net.ontopia.presto.jaxrs.process;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.spi.utils.Utils;

public class SubmittedState {

    private final Map<String,List<TopicView>> topicViews;
    
    public SubmittedState(TopicView rootTopicView) {
        this.topicViews = buildTopicViewsMap(rootTopicView);
    }
    
    public SubmittedState(Collection<TopicView> rootTopicViews) {
        this.topicViews = buildTopicViewsMap(rootTopicViews);
    }
    
    private Map<String, List<TopicView>> buildTopicViewsMap(Collection<TopicView> topicViews) {
        Map<String,List<TopicView>> result = new HashMap<String,List<TopicView>>();
        for (TopicView topicView : topicViews) {
            buildTopicViewsMap(topicView, result);
        }
        return result;
    }

    private Map<String, List<TopicView>> buildTopicViewsMap(TopicView topicView) {
        Map<String,List<TopicView>> result = new HashMap<String,List<TopicView>>();
        buildTopicViewsMap(topicView, result);
        return result;
    }
    
    private void buildTopicViewsMap(TopicView topicView, Map<String,List<TopicView>> result) {
        // register current topic view
        String topicId = topicView.getTopicId();
        Utils.validateNotNull(topicId);

        List<TopicView> list = result.get(topicId);
        if (list == null) {
            list = new ArrayList<TopicView>();
            result.put(topicId, list);
        }
        list.add(topicView);
        
        // register nested topic views
        for (FieldData fd : topicView.getFields()) {
            for (Value v : fd.getValues()) {
                TopicView embedded = v.getEmbedded();
                if (embedded != null) {
                    buildTopicViewsMap(embedded, result);
                }
            }
        }
    }
    
    public List<? extends Object> getValues(String topicId, String fieldId) {
        List<TopicView> list = topicViews.get(topicId);
        if (list != null && !list.isEmpty()) {
            return getValues(list, fieldId);
        } 
        return null;
    }

    private List<? extends Object> getValues(List<TopicView> topicViews, String fieldId) {
        List<Object> result = null;
        for (TopicView topicView : topicViews) {
            for (FieldData fd : topicView.getFields()) {
                if (fieldId.equals(fd.getId())) {
                    for (Value v : fd.getValues()) {
                        String value = v.getValue();
                        if (value != null) {
                            if (result == null) {
                                result = new ArrayList<Object>();
                            }
                            result.add(value);
                        }
                    }
                }
            }
        }
        return result;
    }
    
}
