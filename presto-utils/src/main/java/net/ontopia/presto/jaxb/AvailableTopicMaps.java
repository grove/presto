package net.ontopia.presto.jaxb;

import java.util.Collection;
import java.util.Collections;

import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class AvailableTopicMaps {

    private String id;
    private String name;
    
    private Collection<Link> links;
    private Collection<TopicMap> topicMaps = Collections.emptySet();

    public AvailableTopicMaps() {        
    }
    
    public AvailableTopicMaps(String id, String name) {
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

    public void setTopicMaps(Collection<TopicMap> topicmaps) {
      this.topicMaps = topicmaps;
    }

    public Collection<TopicMap> getTopicMaps() {
      return topicMaps;
    }

}
