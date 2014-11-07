package net.ontopia.presto.jaxb;

import java.util.Collection;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@XmlRootElement
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class LinkList {
    
    private Collection<Link> links;

    public Collection<Link> getLinks() {
        return links;
    }

    public void setLinks(Collection<Link> links) {
        this.links = links;
    }

}
