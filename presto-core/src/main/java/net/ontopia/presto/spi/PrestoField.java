package net.ontopia.presto.spi;

public interface PrestoField {

    String getId();

    String getActualId();
    
    PrestoSchemaProvider getSchemaProvider();

    String getName();

    boolean isNameField(); // TODO: Move into extras

    boolean isPrimitiveField();

    boolean isReferenceField();

    int getMinCardinality();

    int getMaxCardinality();

    String getDataType();

    String getValidationType(); // ISSUE: or getSecondaryType()

    boolean isEmbedded();

    boolean isHidden();

    boolean isTraversable();

    boolean isReadOnly();

    boolean isSorted();

    boolean isPageable();

    boolean isCascadingDelete();

    // reference fields

    boolean isCreatable();
    
    boolean isAddable();
    
    boolean isRemovable();
    
    String getInverseFieldId();

    String getInterfaceControl();

    Object getExtra();

}
