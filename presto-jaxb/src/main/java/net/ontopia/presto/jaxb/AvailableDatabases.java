package net.ontopia.presto.jaxb;

import java.util.Collection;
import java.util.Collections;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class AvailableDatabases extends Document {

    private String name;

    private Collection<Link> links;
    private Collection<Database> databases = Collections.emptySet();

    public AvailableDatabases() {        
    }

    @Override
    public String getFormat() {
        return "available-databases";
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

    public void setDatabases(Collection<Database> databases) {
        this.databases = databases;
    }

    public Collection<Database> getDatabases() {
        return databases;
    }

}
