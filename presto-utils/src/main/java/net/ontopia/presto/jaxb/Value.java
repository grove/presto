package net.ontopia.presto.jaxb;

import java.util.Collection;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Value {

    private Boolean removable;

    // primitive
    private String value;

    // reference
    private String name;
    private Collection<Link> links;
    private Topic embedded;
    private Collection<Value> values;

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
    public void setEmbedded(Topic embedded) {
        this.embedded = embedded;
    }
    public Topic getEmbedded() {
        return embedded;
    }
    
    public Collection<Value> getValues() {
        return values;
    }
    public void setValues(Collection<Value> values) {
        this.values = values;
    }
    
    

}
