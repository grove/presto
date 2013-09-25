package net.ontopia.presto.jaxb;

import java.util.Collection;

import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.map.annotate.JsonSerialize;

@XmlRootElement
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Deprecated
public class AvailableTopicTypes extends Document {

    private Collection<TopicTypeTree> types;

    @Override
    public String getFormat() {
        return "available-topic-types";
    }

    public void setTypes(Collection<TopicTypeTree> types) {
        this.types = types;
    }

    public Collection<TopicTypeTree> getTypes() {
        return types;
    }

}
