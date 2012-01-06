package net.ontopia.presto.jaxb;

import java.util.Collection;

import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class RootInfo extends Document {

    private String name;
    private int version;

    private Collection<Link> links;

    public RootInfo() {        
    }

    @Override
    public String getFormat() {
        return "root-info";
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

    public void setVersion(int version) {
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

}
