package net.ontopia.presto.jaxb;

import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@XmlRootElement
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class TopicMessage {

    private String type;
    private String title;
    private String message;
    private Map<String, Object> params;

    public TopicMessage() {
    }

    public TopicMessage(String type, String title, String message) {
        this.type = type;
        this.title = title;
        this.message = message;
    }
    
    public String getType() {
        return type;
    }
    
    public String getFormat() {
        return "message";
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

}
