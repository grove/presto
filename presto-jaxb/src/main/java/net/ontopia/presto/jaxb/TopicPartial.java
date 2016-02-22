package net.ontopia.presto.jaxb;

import java.util.Collection;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@XmlRootElement
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TopicPartial {
    private String format = "topic-partial";
    private Map<String, Object> params;
    private Collection<TopicView> views;

    public Collection<TopicView> getViews() {
        return views;
    }

    public void setViews(Collection<TopicView> views) {
        this.views = views;
    }

    public String getFormat() {
        return format;
    }
    
    public void setFormat(String format) {
        this.format = format;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

}
