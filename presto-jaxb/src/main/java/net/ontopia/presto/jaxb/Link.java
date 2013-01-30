package net.ontopia.presto.jaxb;

import java.util.Map;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Link {
    
    private String id;
    private String name;
    
    private String rel;
    private String href;

    private Map<String,Object> params;

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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String,Object> getParams() {
        return params;
    }
    
    public void setParams(Map<String,Object> params) {
        this.params = params;
    }

    @Override
    public int hashCode() {
        int result = 0;
        if (id != null) {
            result += id.hashCode();
        }            
        if (rel != null) {
            result += rel.hashCode();
        }            
        if (href != null) {
            result += href.hashCode();
        }            
        if (params != null) {
            result += params.hashCode();
        }            
        if (name != null) {
            result += name.hashCode();
        }            
        return result; 
    }
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof Link) {
            Link o = (Link)other;
            
            return isEqual(id, o.id) && 
                    isEqual(rel, o.rel) && 
                    isEqual(href, o.href) && 
                    isEqual(params, o.params) && 
                    isEqual(name, o.name);
        }
        return false;
    }
    
    public static boolean isEqual(Object o1, Object o2) {
        return (o1 == null ? o2 == null : o1.equals(o2));
    }

}