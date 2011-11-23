package net.ontopia.presto.jaxb;

import java.util.Collection;

import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.map.annotate.JsonSerialize;

@XmlRootElement
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class AvailableTopicTypes {
    
    public static final String MEDIA_TYPE = "application/vnd.presto-available-topic-type+json;charset=UTF-8";

    private Collection<TopicTypeTree> types;

    public void setTypes(Collection<TopicTypeTree> types) {
        this.types = types;
    }

    public Collection<TopicTypeTree> getTypes() {
        return types;
    }

}
