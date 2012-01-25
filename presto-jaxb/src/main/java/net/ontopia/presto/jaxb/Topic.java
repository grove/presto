package net.ontopia.presto.jaxb;

import java.util.Collection;

import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@XmlRootElement
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Topic extends Document {

    private String id;
    private String name;
    private String mode;
    
    private Boolean errors;

    private Origin origin;

    private TopicType type;
    private String view;

    private Collection<Link> links;

    private Collection<FieldData> fields;
    
    @Override
    public String getFormat() {
        return "topic";
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

    public void setType(TopicType type) {
        this.type = type;
    }

    public TopicType getType() {
        return type;
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
        this.links = links;
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

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

}
