package net.ontopia.presto.jaxb;

import java.util.Collection;
import java.util.Collections;

import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class TopicMap {

    private String id;
    private String name;

    private Collection<Link> links = Collections.emptySet();

    public TopicMap() {        
    }
    
    public TopicMap(String id, String name) {
        this.id = id;
        this.name = name;
    }
    
    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
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

}
