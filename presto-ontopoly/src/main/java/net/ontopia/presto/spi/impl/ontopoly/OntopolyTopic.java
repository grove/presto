package net.ontopia.presto.spi.impl.ontopoly;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.ontopia.infoset.core.LocatorIF;
import net.ontopia.topicmaps.core.OccurrenceIF;
import net.ontopia.topicmaps.core.TopicNameIF;
import ontopoly.model.FieldDefinition;
import ontopoly.model.RoleField;
import ontopoly.model.Topic;
import ontopoly.model.TopicType;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import ontopoly.utils.OntopolyUtils;

public class OntopolyTopic implements PrestoTopic {

  private final OntopolySession session;
  private final Topic topic;

  OntopolyTopic(OntopolySession session, Topic topic) {
    this.session = session;
    this.topic = topic;    
  }
  
  public boolean equals(Object o) {
    if (o instanceof OntopolyTopic) {
      OntopolyTopic other = (OntopolyTopic)o;
      return other.topic.equals(this.topic);
    }
    return false;
  }
  
  public int hashCode() {
    return this.topic.hashCode();
  }
  
  static Topic getWrapped(PrestoTopic topic) {
    return ((OntopolyTopic)topic).topic;
  }

  public String getId() {
    return session.getStableId(topic);
  }

  public String getName() {
    return topic.getName();
  }
  
  public String getTypeId() {
    TopicType defaultTopicType = OntopolyUtils.getDefaultTopicType(topic);
    return session.getStableId(defaultTopicType);
  }

  static Collection<PrestoTopic> wrap(OntopolySession session, Collection<Topic> topics) {
    List<PrestoTopic> result = new ArrayList<PrestoTopic>(topics.size());
    for (Topic topic : topics) {
      result.add(new OntopolyTopic(session, topic));
    }
    return result;
  }
  
  public Collection<Object> getValues(PrestoField field) {
    FieldDefinition fieldDefinition = FieldDefinition.getFieldDefinition(session.getTopicById(field.getId()), session.getTopicMap());
    
    Collection<? extends Object> fieldValues = fieldDefinition.getValues(topic);
    List<Object> result = new ArrayList<Object>(fieldValues.size());
    if (fieldDefinition.getFieldType() == FieldDefinition.FIELD_TYPE_ROLE && 
        ((RoleField)fieldDefinition).getAssociationField().getArity() == 1) {
      result.add(!fieldValues.isEmpty());
    } else {
      for (Object value : fieldValues) {
        result.add(normalizeValue(topic, fieldDefinition, value));
      }
    }
    return result;
  }

  private  Object normalizeValue(Topic topic, FieldDefinition fieldDefinition, Object fieldValue) {
    switch (fieldDefinition.getFieldType()) {
    case FieldDefinition.FIELD_TYPE_NAME:
      return ((TopicNameIF)fieldValue).getValue();
    case FieldDefinition.FIELD_TYPE_IDENTITY: 
      return ((LocatorIF)fieldValue).getExternalForm();
    case FieldDefinition.FIELD_TYPE_OCCURRENCE:
      return ((OccurrenceIF)fieldValue).getValue();
    case FieldDefinition.FIELD_TYPE_ROLE:
      RoleField roleField = (RoleField)fieldDefinition;
      RoleField.ValueIF value = (RoleField.ValueIF)fieldValue;
      int arity = value.getArity(); 
      if (arity == 2) {
        for (RoleField rf : value.getRoleFields()) {
          if (!rf.equals(roleField)) {
            Topic valueTopic = value.getPlayer(rf, topic);
            return new OntopolyTopic(session, valueTopic);
          }
        }
        return null;
      } else {
        throw new RuntimeException("N-ary role fields not supported.");
      }
    case FieldDefinition.FIELD_TYPE_QUERY: 
      if (fieldValue instanceof Topic) {        
        return new OntopolyTopic(session, (Topic)fieldValue);
      } else {
        return fieldValue;
      }
    default:
      throw new RuntimeException("Unknown field type: " + fieldDefinition);
    }
  }

}
