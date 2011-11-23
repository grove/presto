package net.ontopia.presto.jaxb;

import java.util.Collection;

import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class Database {
    
    public static final String MEDIA_TYPE = "application/vnd.presto-database+json;charset=UTF-8";

    private String id;
    private String name;

    private Collection<Link> links;

    public Database() {        
    }

    public Database(String id, String name) {
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
