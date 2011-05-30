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
    private PrestoSchemaProvider schemaProvider;
    private String name;
    private boolean isNameField;
    private PrestoView valueView;
    private int minCardinality;
    private int maxCardinality;
    private String dataType;
    private String validationType;
    private boolean isEmbedded;
    private boolean isHidden;
    private boolean isTraversable = true;
    private boolean isReadOnly;
    private boolean isSorted;
    private boolean isCascadingDelete;
    private boolean isNewValuesOnly;
    private boolean isExistingValuesOnly;
    private String inverseFieldId;
    private String interfaceControl;
    private Object extra;

    private Collection<PrestoType> availableFieldCreateTypes;
    private Collection<PrestoType> availableFieldValueTypes = new HashSet<PrestoType>();

    // helper members
    private Collection<PrestoView> definedInViews = new HashSet<PrestoView>();
    
    PojoField(String id, PrestoSchemaProvider schemaProvider) {
        this.id = id;
        this.schemaProvider = schemaProvider;        
    }

    public String getId() {
        return id;
    }

    public PrestoSchemaProvider getSchemaProvider() {
        return schemaProvider;
    }

    public String getName() {
        return name;
    }

    public boolean isNameField() {
        return isNameField;
    }

    public boolean isPrimitiveField() {
        return !isReferenceField();
    }

    public boolean isReferenceField() {
        return dataType != null && dataType.equals("reference");
    }

    public PrestoView getValueView() {
        return valueView;
    }

    public int getMinCardinality() {
        return minCardinality;
    }

    public int getMaxCardinality() {
        return maxCardinality;
    }

    public String getDataType() {
        return dataType;
    }

    public String getValidationType() {
        return validationType;
    }

    public boolean isEmbedded() {
        return isEmbedded;
    }

    public boolean isHidden() {
        return isHidden;
    }

    public boolean isTraversable() {
        return isTraversable;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public boolean isSorted() {
        return isSorted;
    }

    public boolean isCascadingDelete() {
        return isCascadingDelete;
    }

    public boolean isNewValuesOnly() {
        return isNewValuesOnly;
    }

    public boolean isExistingValuesOnly() {
        return isExistingValuesOnly;
    }

    public String getInverseFieldId() {
        return inverseFieldId;
    }

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
        for (PrestoView definedInView : definedInViews) {
            if (definedInView.equals(view)) {
                return true;
            }
        }
        return false;
    }

    protected void addDefinedInView(PrestoView view) {
        this.definedInViews.add(view);
    }
    
    public void setName(String name) {
        this.name = name;
    }

    public void setNameField(boolean isNameField) {
        this.isNameField = isNameField;
    }

    public void setValueView(PrestoView valueView) {
        this.valueView = valueView;
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

    public void setCascadingDelete(boolean isCascadingDelete) {
        this.isCascadingDelete = isCascadingDelete;
    }

    public void setNewValuesOnly(boolean isNewValuesOnly) {
        this.isNewValuesOnly = isNewValuesOnly;
    }

    public void setExistingValuesOnly(boolean isExistingValuesOnly) {
        this.isExistingValuesOnly = isExistingValuesOnly;
    }

    public void setInverseFieldId(String inverseFieldId) {
        this.inverseFieldId = inverseFieldId;
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

    public Object getExtra() {
        return extra;
    }

}
