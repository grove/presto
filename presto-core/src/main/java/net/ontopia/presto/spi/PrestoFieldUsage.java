package net.ontopia.presto.spi;

import java.util.Collection;

public interface PrestoFieldUsage extends PrestoField {

    PrestoType getType();

    PrestoView getView();

    PrestoView getValueView(PrestoType type);

    PrestoView getEditView(PrestoType type);

    PrestoView getCreateView(PrestoType type);

    // The types of objects that can be created and used as values of the field. Must be a subset of availableFieldValueTypes.
    Collection<PrestoType> getAvailableFieldCreateTypes();

    // Used to tell client what kinds of values may be added to this field. 
    // Also used by some implementations to return a list of available field values.
    // ISSUE: can this be made internal? 
    Collection<PrestoType> getAvailableFieldValueTypes();

}
