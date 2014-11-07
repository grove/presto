package net.ontopia.presto.jaxb;

import java.util.Collection;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@XmlRootElement
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TopicViewPartial {
    
    private String format = "topic-view-partial";
    private Collection<FieldData> fields;

    public String getFormat() {
        return format;
    }
    
    public void setFormat(String format) {
        this.format = format;
    }

    public Collection<FieldData> getFields() {
        return fields;
    }

    public void setFields(Collection<FieldData> fields) {
        this.fields = fields;
    }

}
