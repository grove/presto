package net.ontopia.presto.spi;

import java.util.Collection;

public interface PrestoField {

    String getId();

    String getActualId();
    
    PrestoSchemaProvider getSchemaProvider();

    String getName();

    boolean isNameField(); // TODO: Move into extras

    boolean isReferenceField();

    String getDataType();

    String getValidationType(); // ISSUE: or getSecondaryType()

    String getInterfaceControl();
    
    int getMinCardinality();
    
    int getMaxCardinality();

    Object getExtra();
    
    String getInverseFieldId();

    // inherent characteristics
    
    boolean isInline();

    String getInlineReference();

    boolean isParentRelation();
    
    boolean isEmbedded();

    // characteristics

    boolean isHidden();

    boolean isTraversable();

    boolean isSorted();

    boolean isSortedAscending();

    boolean isPageable();

    // mutability

    boolean isReadOnly();
    
    boolean isEditable();
    
    boolean isCreatable();
    
    boolean isAddable();
    
    boolean isRemovable();
    
    boolean isMovable();

    boolean isCascadingDelete();

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
