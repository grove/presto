package net.ontopia.presto.spi;

public interface PrestoField {

    String getId();

    PrestoSchemaProvider getSchemaProvider();

    String getName();

    boolean isNameField(); // TODO: Move into extras

    boolean isPrimitiveField();

    boolean isReferenceField();

    int getMinCardinality();

    int getMaxCardinality();

    String getDataType();

    String getValidationType(); // ISSUE: or concreteType/actualType?

    boolean isEmbedded();

    boolean isHidden();

    boolean isTraversable();

    boolean isReadOnly();

    boolean isSorted();

    boolean isPageable();

    boolean isCascadingDelete();

    // reference fields

    boolean isNewValuesOnly();

    boolean isExistingValuesOnly();

    String getInverseFieldId();

    String getInterfaceControl();

    Object getExtra();

}
