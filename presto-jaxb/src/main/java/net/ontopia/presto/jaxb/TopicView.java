package net.ontopia.presto.jaxb;

import java.util.Collection;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@XmlRootElement
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class TopicView {
    
    private String id;
    private String name;
    private String href;
    private String mode;

    private String topicId; // TODO: move to params?
    private String topicTypeId; // TODO: move to params?
    
    private String format;
    private Layout layout;

    private Origin origin;

    private Map<String, Object> params;
    private Collection<Link> links;
    private Collection<FieldData> fields;
    
    public static TopicView remoteView() {
        TopicView topicView = new TopicView();
        topicView.setFormat("topic-view-remote");
        return topicView;
    }
    
    public static TopicView view() {
        TopicView topicView = new TopicView();
        topicView.setFormat("topic-view");
        return topicView;
    }
   
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    
    public String getFormat() {
        return format;
    }
    public void setFormat(String format) {
        this.format = format;
    }
    
    public Layout getLayout() {
        return layout;
    }
    public void setLayout(Layout layout) {
        this.layout = layout;
    }
    
    public Map<String, Object> getParams() {
        return params;
    }
    public void setParams(Map<String, Object> params) {
        this.params = params;
    }
    
    public Collection<Link> getLinks() {
        return links;
    }
    public void setLinks(Collection<Link> links) {
        this.links = links;
    }
    
    public Collection<FieldData> getFields() {
        return fields;
    }
    public void setFields(Collection<FieldData> fields) {
        this.fields = fields;
    }
    
    public String getHref() {
        return href;
    }
    public void setHref(String href) {
        this.href = href;
    }

    public String getMode() {
        return mode;
    }
    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getTopicId() {
        return topicId;
    }

    public void setTopicId(String topicId) {
        this.topicId = topicId;
    }
    
    public Origin getOrigin() {
        return origin;
    }

    public void setOrigin(Origin origin) {
        this.origin = origin;
    }

    public String getTopicTypeId() {
        return topicTypeId;
    }

    public void setTopicTypeId(String topicTypeId) {
        this.topicTypeId = topicTypeId;
    }

}
