package net.ontopia.presto.jaxb;

import java.util.Collection;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Value {

    private Boolean editable; // TODO: move to params
    private Boolean removable; // TODO: move to params

    // primitive
    private String value;

    // reference
    private String name;
    private Collection<Link> links;
    private TopicView embedded;
    private Collection<Value> values;

    private Map<String,Object> params;

    public Value() {
    }
    
    public static Value getNullValue() {
        Value result = new Value();
        result.setValue(null);
        result.setName(null);
        return result;
    }
    
    public void setEditable(Boolean editable) {
        this.editable = editable;
    }

    public Boolean isEditable() {
        return editable;
    }

    public void setRemovable(Boolean removable) {
        this.removable = removable;
    }

    public Boolean isRemovable() {
        return removable;
    }
    
    public void setValue(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return value;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
    
    public void setLinks(Collection<Link> links) {
        if (links.isEmpty()) {
            this.links = null;
        } else {
            this.links = links;
        }
    }
    
    public Collection<Link> getLinks() {
        return links;
    }
    
    public void setEmbedded(TopicView embedded) {
        this.embedded = embedded;
    }
    
    public TopicView getEmbedded() {
        return embedded;
    }
    
    public Collection<Value> getValues() {
        return values;
    }
    
    public void setValues(Collection<Value> values) {
        this.values = values;
    }

    public Map<String,Object> getParams() {
        return params;
    }

    public void setParams(Map<String,Object> params) {
        this.params = params;
    }
    
    

}
