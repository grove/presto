package net.ontopia.presto.jaxb;

import java.util.Collection;
import java.util.HashSet;

public class TopicUtils {

    public static Topic asTopic(TopicFields f, TopicViews v) {
        Topic topic = new Topic();

        topic.setId(v.getId());
        topic.setName(v.getName());
        topic.setView(v.getView());
        topic.setMode(v.getMode());
        
        topic.setType(f.getType());
        topic.setOrigin(f.getOrigin());
        
        topic.setErrors(f.getErrors());
        topic.setFields(f.getFields());
        
        Collection<Link> links = new HashSet<Link>();
        links.addAll(f.getLinks());
        links.addAll(v.getLinks());
        topic.setLinks(links);
        
        return topic;
    }
    
    public static TopicFields asTopicFields(Topic t) {
        TopicFields fields = new TopicFields();
        fields.setId(t.getId());
        fields.setName(t.getName());
        fields.setType(t.getType());
        fields.setOrigin(t.getOrigin());
        fields.setView(t.getView());
        fields.setErrors(t.getErrors());
        fields.setFields(t.getFields());
        fields.setLinks(t.getLinks());
        return fields;
    }

    public static TopicViews asTopicViews(Topic t) {
        TopicViews views = new TopicViews();
        views.setId(t.getId());
        views.setName(t.getName());
        views.setMode(t.getMode());
        views.setView(t.getView());
        views.setLinks(t.getLinks());
        return views;
    }
    
}
