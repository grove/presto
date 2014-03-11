package net.ontopia.presto.spi.impl.pojo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;

public class PojoField implements PrestoField {

    private String id;
    private String actualId;

    private PojoType type;
    private PojoView view;

    private PrestoSchemaProvider schemaProvider;
    private String name;
    private boolean isNameField;
    private String valueViewId;
    private String editViewId;
    private String createViewId;
    private int minCardinality;
    private int maxCardinality;
    private String dataType;
    private String validationType;
    private boolean isInline;
    private boolean isEmbedded;
    private boolean isHidden;
    private boolean isTraversable = true;
    private boolean isReadOnly;
    private boolean isSorted;
    private boolean isSortedAscending = true;
    private boolean isPageable;
    private int limit;
    private boolean isCascadingDelete;
    private String inverseFieldId;
    private boolean isEditable = true;
    private boolean isCreatable = true;
    private boolean isAddable = true;
    private boolean isRemovable = true;
    private boolean isMovable = true;
    private String interfaceControl;
    private Object extra;

    private Collection<PrestoType> availableFieldCreateTypes;
    private Collection<PrestoType> availableFieldValueTypes = new HashSet<PrestoType>();
    private String inlineReference;

    PojoField(String id, PojoType type, PojoView view, PrestoSchemaProvider schemaProvider) {
        this.id = id;
        this.type = type;
        this.view = view;
        this.actualId = id;
        this.schemaProvider = schemaProvider;        
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getActualId() {
        return actualId;
    }

    @Override
    public PrestoSchemaProvider getSchemaProvider() {
        return schemaProvider;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isNameField() {
        return isNameField;
    }

    @Override
    public boolean isReferenceField() {
        return dataType != null && dataType.equals("reference");
    }

    public PrestoView getValueView(PrestoType type) {
        if (valueViewId != null) {
            return type.getViewById(valueViewId);
        } else {
            return type.getDefaultView();
        }
    }

    public PrestoView getEditView(PrestoType type) {
        if (editViewId != null) {
            return type.getViewById(editViewId);
        } else {
            return type.getDefaultView();
        }
    }

    public PrestoView getCreateView(PrestoType type) {
        if (createViewId != null) {
            return type.getViewById(createViewId);
        } else {
            return type.getCreateView();
        }
    }

    @Override
    public int getMinCardinality() {
        return minCardinality;
    }

    @Override
    public int getMaxCardinality() {
        return maxCardinality;
    }

    @Override
    public String getDataType() {
        return dataType;
    }

    @Override
    public String getValidationType() {
        return validationType;
    }

    @Override
    public boolean isInline() {
        return isInline;
    }

    @Override
    public boolean isEmbedded() {
        return isEmbedded;
    }

    @Override
    public boolean isHidden() {
        return isHidden;
    }

    @Override
    public boolean isTraversable() {
        return isTraversable;
    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    @Override
    public boolean isSorted() {
        return isSorted;
    }

    @Override
    public boolean isSortedAscending() {
        return isSortedAscending;
    }

    @Override
    public boolean isPageable() {
        return isPageable;
    }

    // TODO: rename to pageSize?
    public int getLimit() {
        return limit;
    }

    @Override
    public boolean isCascadingDelete() {
        return isCascadingDelete;
    }
    
    @Override
    public boolean isEditable() {
        return isEditable;
    }
    
    @Override
    public boolean isCreatable() {
        return isCreatable;
    }

    @Override
    public boolean isAddable() {
        return isAddable;
    }

    @Override
    public boolean isMovable() {
        return isMovable;
    }

    @Override
    public boolean isRemovable() {
        return isRemovable;
    }

    @Override
    public String getInterfaceControl() {
        return interfaceControl;
    }

    public Collection<PrestoType> getAvailableFieldCreateTypes() {
        // fall back to creatable value types if no create types are specified
        if (availableFieldCreateTypes == null) {
            List<PrestoType> result = new ArrayList<PrestoType>(availableFieldValueTypes.size());
            for (PrestoType valueType : availableFieldValueTypes) {
                if (valueType.isCreatable()) {
                    result.add(valueType);
                }
            }
            return result;
        } else {
            return availableFieldCreateTypes;
        }
    }

    public Collection<PrestoType> getAvailableFieldValueTypes() {
        return availableFieldValueTypes;
    }

    // -- helper methods

    boolean isInView(PrestoView view) {
        return this.view.equals(view);
    }

    public void setActualId(String actualId) {
        this.actualId = actualId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setNameField(boolean isNameField) {
        this.isNameField = isNameField;
    }

    public void setValueViewId(String valueViewId) {
        this.valueViewId = valueViewId;
    }

    public void setEditViewId(String editViewId) {
        this.editViewId = editViewId;
    }

    public void setCreateViewId(String createViewId) {
        this.createViewId = createViewId;
    }

    public void setMinCardinality(int minCardinality) {
        this.minCardinality = minCardinality;
    }

    public void setMaxCardinality(int maxCardinality) {
        this.maxCardinality = maxCardinality;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public void setValidationType(String validationType) {
        this.validationType = validationType;
    }

    public void setInline(boolean isInline) {
        this.isInline = isInline;
    }

    public void setEmbedded(boolean isEmbedded) {
        this.isEmbedded = isEmbedded;
    }

    public void setHidden(boolean isHidden) {
        this.isHidden = isHidden;
    }

    public void setTraversable(boolean isTraversable) {
        this.isTraversable = isTraversable;
    }

    public void setReadOnly(boolean isReadOnly) {
        this.isReadOnly = isReadOnly;
    }

    public void setSorted(boolean isSorted) {
        this.isSorted = isSorted;
    }

    public void setSortAscending(boolean isSortAscending) {
        this.isSortedAscending = isSortAscending;
    }

    public void setPageable(boolean isPageable) {
        this.isPageable = isPageable;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public void setCascadingDelete(boolean isCascadingDelete) {
        this.isCascadingDelete = isCascadingDelete;
    }
    
    public void setEditable(boolean isEditable) {
        this.isEditable = isEditable;
    }
    
    public void setCreatable(boolean isCreatable) {
        this.isCreatable = isCreatable;
    }

    public void setAddable(boolean isAddable) {
        this.isAddable = isAddable;
    }

    public void setRemovable(boolean isRemovable) {
        this.isRemovable = isRemovable;
    }

    public void setMovable(boolean isMovable) {
        this.isMovable = isMovable;
    }

    public void setInterfaceControl(String interfaceControl) {
        this.interfaceControl = interfaceControl;
    }

    protected void setAvailableFieldCreateType(Set<PrestoType> createTypes) {
        this.availableFieldCreateTypes = createTypes;
    }

    protected void setAvailableFieldValueType(Set<PrestoType> valueTypes) {
        this.availableFieldValueTypes = valueTypes;
    }

    public void setExtra(Object extra) {
        this.extra = extra;
    }

    @Override
    public Object getExtra() {
        return extra;
    }

    @Override
    public String getInverseFieldId() {
        return inverseFieldId;
    }
    
    void setInverseFieldId(String inverseFieldId) {
        this.inverseFieldId = inverseFieldId;
    }

    @Override
    public String toString() {
        return "PojoField[" + getId() + "|" + getName() + "]";
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
    public boolean equals(Object other) {
        if (other instanceof PojoField) {
            PojoField o = (PojoField)other;
            return o.getId().equals(o.getId());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public String getInlineReference() {
        return inlineReference;
    }
    
    void setInlineReference(String inlineReference) {
        this.inlineReference = inlineReference;
    }

}
