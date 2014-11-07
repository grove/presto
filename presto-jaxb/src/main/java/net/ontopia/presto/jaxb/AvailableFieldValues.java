package net.ontopia.presto.jaxb;

import java.util.Collection;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@XmlRootElement
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class AvailableFieldValues extends Document {

    private String id;
    private String name;
    private Collection<Value> values;

    @Override
    public String getFormat() {
        return "available-field-values";
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

    public void setValues(Collection<Value> values) {
        this.values = values;
    }

    public Collection<Value> getValues() {
        return values;
    }

}
