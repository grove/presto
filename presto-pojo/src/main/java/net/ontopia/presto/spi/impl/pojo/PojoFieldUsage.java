package net.ontopia.presto.spi.impl.pojo;

import java.util.Collection;

import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;

public class PojoFieldUsage implements PrestoFieldUsage {

    private final PojoField field;
    private final PrestoType type;
    private final PrestoView view;

    PojoFieldUsage(PojoField field, PrestoType type, PrestoView view) {
        this.field = field;
        this.type = type;
        this.view = view;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof PojoFieldUsage) {
            PojoFieldUsage o = (PojoFieldUsage)other;
            return field.equals(o.field) && type.equals(o.type) && view.equals(o.view);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return field.hashCode() + type.hashCode() + view.hashCode();
    }

    @Override
    public PrestoType getType() {
        return type;
    }

    @Override
    public PrestoView getView() {
        return view;
    }

    @Override
    public String getId() {
        return field.getId();
    }

    @Override
    public String getActualId() {
        return field.getActualId();
    }

    @Override
    public PrestoSchemaProvider getSchemaProvider() {
        return field.getSchemaProvider();
    }

    @Override
    public String getName() {
        return field.getName();
    }

    @Override
    public boolean isNameField() {
        return field.isNameField();
    }

    @Override
    public boolean isReferenceField() {
        return field.isReferenceField();
    }

    @Override
    public PrestoView getValueView(PrestoType type) {
        return field.getValueView(type);
    }

    @Override
    public PrestoView getEditView(PrestoType type) {
        return field.getEditView(type);
    }

    @Override
    public PrestoView getCreateView(PrestoType type) {
        return field.getCreateView(type);
    }

    @Override
    public int getMinCardinality() {
        return field.getMinCardinality();
    }

    @Override
    public int getMaxCardinality() {
        return field.getMaxCardinality();
    }

    @Override
    public String getDataType() {
        return field.getDataType();
    }

    @Override
    public String getValidationType() {
        return field.getValidationType();
    }

    @Override
    public boolean isInline() {
        return field.isInline();
    }

    @Override
    public boolean isEmbedded() {
        return field.isEmbedded();
    }

    @Override
    public boolean isHidden() {
        return field.isHidden();
    }

    @Override
    public boolean isTraversable() {
        return field.isTraversable();
    }

    @Override
    public boolean isReadOnly() {
        return field.isReadOnly();
    }

    @Override
    public boolean isSorted() {
        return field.isSorted();
    }

    @Override
    public boolean isSortedAscending() {
        return field.isSortedAscending();
    }

    @Override
    public boolean isPageable() {
        return field.isPageable();
    }

    @Override
    public boolean isCascadingDelete() {
        return field.isCascadingDelete();
    }
    
    @Override
    public boolean isEditable() {
        return field.isEditable();
    }
    
    @Override
    public boolean isCreatable() {
        return field.isCreatable();
    }

    @Override
    public boolean isAddable() {
        return field.isAddable();
    }

    @Override
    public boolean isRemovable() {
        return field.isRemovable();
    }

    @Override
    public boolean isMovable() {
        return field.isMovable();
    }

    @Override
    public String getInterfaceControl() {
        return field.getInterfaceControl();
    }

    @Override
    public Collection<PrestoType> getAvailableFieldCreateTypes() {
        return field.getAvailableFieldCreateTypes();
    }

    @Override
    public Collection<PrestoType> getAvailableFieldValueTypes() {
        return field.getAvailableFieldValueTypes();
    }

    @Override
    public Object getExtra() {
        return field.getExtra();
    }

    @Override
    public String getInverseFieldId() {
        return field.getInverseFieldId();
    }

    @Override
    public String toString() {
        return "PojoFieldUsage[" + getId() + "|" + getName() + "]";
    }

}
