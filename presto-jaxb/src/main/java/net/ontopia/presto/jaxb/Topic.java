package net.ontopia.presto.jaxb;

import java.util.Collection;
import java.util.Map;

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
    private String format = "topic";
    
    private Origin origin;

    private TopicType type;
    private String view;

    private Map<String,Object> params;

    private Collection<Link> links;

    private Collection<FieldData> fields;

    @Override
    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        if (format.equals("topic") || format.equals("topic-fields")) {
            this.format = format;
        } else {
            throw new IllegalArgumentException("Invalid format: '" + format + "' Expected: 'topic' or 'topic-fields.");
        }
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
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

    public Collection<FieldData> getFields() {
        return fields;
    }

    public void setFields(Collection<FieldData> fields) {
        this.fields = fields;
    }

    public Origin getOrigin() {
        return origin;
    }

    public void setOrigin(Origin origin) {
        this.origin = origin;
    }

    public Collection<Link> getLinks() {
        return links;
    }

    public void setLinks(Collection<Link> links) {
        this.links = links;
    }

    public Map<String,Object> getParams() {
        return params;
    }

    public void setParams(Map<String,Object> params) {
        this.params = params;
    }

}
