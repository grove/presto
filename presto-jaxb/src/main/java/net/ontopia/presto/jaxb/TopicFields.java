package net.ontopia.presto.jaxb;

import java.util.Collection;

import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@XmlRootElement
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class TopicFields extends Document {

    private String id;
    private String name;
    private String view;

    private Origin origin;

    private Collection<Link> links;
    
    private Boolean errors;

    private Collection<FieldData> fields;

    private String format = "topic-fields";
    
    @Override
    public String getFormat() {
        return format;
    }

    @Override
    public void setFormat(String format) {
        this.format = format;
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

    public void setView(String view) {
        this.view = view;
    }

    public String getView() {
        return view;
    }

    public void setFields(Collection<FieldData> fields) {
        this.fields = fields;
    }

    public Collection<FieldData> getFields() {
        return fields;
    }

    public void setOrigin(Origin origin) {
        this.origin = origin;
    }

    public Origin getOrigin() {
        return origin;
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

    public Boolean getErrors() {
        return errors;
    }

    public void setErrors(Boolean errors) {
        this.errors = errors;
    }

}
