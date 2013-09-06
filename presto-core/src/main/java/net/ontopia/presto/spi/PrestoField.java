package net.ontopia.presto.spi;

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

    boolean isCascadingDelete();

}
