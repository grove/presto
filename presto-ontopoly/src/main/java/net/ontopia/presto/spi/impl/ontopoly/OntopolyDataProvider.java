package net.ontopia.presto.spi.impl.ontopoly;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import ontopoly.model.EditMode;
import ontopoly.model.FieldDefinition;
import ontopoly.model.FieldsView;
import ontopoly.model.RoleField;
import ontopoly.model.Topic;
import ontopoly.model.ViewModes;
import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

public class OntopolyDataProvider implements PrestoDataProvider {

  OntopolySession session;
  
  public OntopolyDataProvider(OntopolySession session) {
    this.session = session;
  }

  public PrestoTopic getTopicById(String id) {
    Topic topic = session.getTopicMap().getTopicById(id);
    if (topic == null) {
      throw new RuntimeException("Unknown topic: " + id);
    }
    return new OntopolyTopic(session, topic);
  }
  
  public Collection<PrestoTopic> getTopicsByIds(Collection<String> topicIds) {
    List<PrestoTopic> result = new ArrayList<PrestoTopic>(topicIds.size());
    for (String topicId : topicIds) {
      result.add(getTopicById(topicId));
    }
    return result;
  }
  
  public Collection<PrestoTopic> getAvailableFieldValues(PrestoFieldUsage field) {
    FieldDefinition fieldDefinition = FieldDefinition.getFieldDefinition(session.getTopicById(field.getId()), session.getTopicMap());
    
    if (fieldDefinition.getFieldType() == FieldDefinition.FIELD_TYPE_ROLE) {
      RoleField roleField = (RoleField)fieldDefinition;
      int arity = roleField.getAssociationField().getArity();

      if (arity == 2) {

        FieldsView fieldsView = OntopolyView.getWrapped(field.getView());
        FieldsView childView = fieldDefinition.getValueView(fieldsView);    

        EditMode editMode = roleField.getEditMode();
        ViewModes viewModes = fieldDefinition.getViewModes(childView);

        boolean allowAdd = !editMode.isNoEdit() && !editMode.isNewValuesOnly() && !viewModes.isReadOnly();

        for (RoleField otherRoleField : roleField.getOtherRoleFields()) {

          if (allowAdd) {
            return OntopolyTopic.wrap(session, otherRoleField.getAllowedPlayers(null));
          } else {
            return Collections.emptyList();          
          }
        }
      }
    }
    return Collections.emptyList();    
  }

  public PrestoChangeSet createTopic(PrestoType type) {
    return new OntopolyChangeSet(session, type);
  }
  
  public PrestoChangeSet updateTopic(PrestoTopic topic) {
    return new OntopolyChangeSet(session, topic);
  }

  public boolean removeTopic(PrestoTopic topic) {
    OntopolyTopic.getWrapped(topic).remove(null);
    return true;
  }

  public void close() {
    // no-op
  }

}
