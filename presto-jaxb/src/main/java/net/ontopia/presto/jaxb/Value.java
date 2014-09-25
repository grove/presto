package net.ontopia.presto.jaxb;

import java.util.Collection;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Value {

    private Boolean editable;
    private Boolean removable;
    
    private String value;
    private String name;
    private String type;
    
    private Collection<Value> values; // nested values (see FieldData.valueFields)

    private TopicView embedded;

    private Map<String,Object> params;

    private Collection<Link> links;

    public Value() {
    }
    
    @Override
    public String toString() {
        return "[Value: " + value + " " + name + " " + type + "]";
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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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
