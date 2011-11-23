package net.ontopia.presto.jaxb;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Link {
    
    private String name;
    private String type;
    
    private String rel;
    private String href;
    private String mode;

    public Link() {        
    }

    public Link(String rel, String href) {
        this.rel = rel;
        this.href = href;
    }

    public void setRel(String rel) {
        this.rel = rel;
    }
    
    public String getRel() {
        return rel;
    }
    
    public void setHref(String href) {
        this.href = href;
    }
    
    public String getHref() {
        return href;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

}