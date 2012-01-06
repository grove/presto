package net.ontopia.presto.jaxb;

import java.util.Collection;

import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class TopicTypeTree {

    private String id;
    private String name;
    private Collection<TopicTypeTree> types;
    private Collection<Link> links;

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

    public void setTypes(Collection<TopicTypeTree> types) {
        this.types = types;
    }

    public Collection<TopicTypeTree> getTypes() {
        return types;
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
