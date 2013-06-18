package net.ontopia.presto.spi.impl.pojo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;

public class PojoType implements PrestoType {

    private PrestoSchemaProvider schemaProvider;

    private String id;
    private String name;
    private boolean isInline;
    private boolean isHidden;
    private boolean isCreatable = true;
    private boolean isRemovable;
    private boolean isRemovableCascadingDelete;

    private String defaultViewId;
    private String createViewId;

    private Collection<PrestoType> directSubTypes = new HashSet<PrestoType>();

    private List<PrestoField> fields = new ArrayList<PrestoField>();
    private Map<String,PojoField> fieldsMap = new HashMap<String,PojoField>();

    private List<PrestoView> views = new ArrayList<PrestoView>();
    private Map<String,PrestoView> viewsMap = new HashMap<String,PrestoView>();

    private Object extra;

    PojoType(String id, PrestoSchemaProvider schemaProvider) {
        this.id = id;
        this.schemaProvider = schemaProvider;        
    }

    @Override
    public String toString() {
        return "PojoType[" + getId() + "|" + getName() + "]";
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof PojoType) {
            PojoType o = (PojoType)other;
            return id.equals(o.id);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public PrestoSchemaProvider getSchemaProvider() {
        return schemaProvider;
    }

    public boolean isInline() {
        return isInline;
    }

    @Override
    public boolean isHidden() {
        return isHidden;
    }

    @Override
    public boolean isCreatable() {
        return isCreatable;
    }

    @Override
    public boolean isRemovable() {
        return isRemovable;
    }

    @Override
    public boolean isRemovableCascadingDelete() {
        return isRemovableCascadingDelete;
    }

    @Override
    public Collection<PrestoType> getDirectSubTypes() {
        return directSubTypes;
    }

    @Override
    public List<PrestoField> getFields() {
        return fields;
    }

    @Override
    public List<PrestoFieldUsage> getFields(PrestoView fieldsView) {
        List<PrestoFieldUsage> result = new ArrayList<PrestoFieldUsage>();
        for (PrestoField field : fields) {
            PojoField pojoField = (PojoField)field;
            if (pojoField.isInView(fieldsView)) {
                result.add(new PojoFieldUsage(pojoField, this, fieldsView));
            }
        }
        return result;
    }

    @Override
    public PrestoField getFieldById(String fieldId) {
        PojoField field = fieldsMap.get(fieldId);
        if (field == null) {
            throw new RuntimeException("Field '" + fieldId + "' not found on type " + this.getId());
        }
        return field;
    }

    @Override
    public PrestoFieldUsage getFieldById(String fieldId, PrestoView view) {
        PojoField field = fieldsMap.get(fieldId);
        if (field == null) {
            throw new RuntimeException("Field '" + fieldId + "' in view " + view.getId() + " not found on type " + this.getId());
        } else if (!field.isInView(view)) {
            throw new RuntimeException("Field '" + fieldId + "' in not defined in view " + view.getId() + " on type " + this.getId());
        }
        return new PojoFieldUsage(field, this, view);
    }

    @Override
    public PrestoView getDefaultView() {
        return getViewById(defaultViewId);
    }

    @Override
    public PrestoView getCreateView() {
        return getViewById(createViewId);
    }
    
    @Override
    public PrestoView getViewById(String viewId) {
        PrestoView view = viewsMap.get(viewId);
        if (view == null) {
            throw new RuntimeException("View '" + viewId + "' not found on type " + this.getId());
        }
        return view;
    }

    @Override
    public Collection<PrestoView> getViews(PrestoView fieldsView) {
        return views;
    }

    protected void setName(String name) {
        this.name = name;
    }

    protected void setInline(boolean isInline) {
        this.isInline = isInline;
    }

    protected void setHidden(boolean isHidden) {
        this.isHidden = isHidden;
    }

    protected void setCreatable(boolean isCreatable) {
        this.isCreatable = isCreatable;
    }

    protected void setRemovable(boolean isRemovable) {
        this.isRemovable = isRemovable;
    }

    protected void setRemovableCascadingDelete(boolean isRemovableCascadingDelete) {
        this.isRemovableCascadingDelete = isRemovableCascadingDelete;
    }

    protected void addDirectSubType(PrestoType type) {
        this.directSubTypes.add(type);

    }

    protected void addView(PojoView view) {
        if (this.viewsMap.containsKey(view.getId())) {
            throw new RuntimeException("Duplicate view: " + view.getId() + " on type " + getId());
        } else {
            this.viewsMap.put(view.getId(), view);
            this.views.add(view);
        }
    }

    protected void addField(PojoField field) {
        if (this.fieldsMap.containsKey(field.getId())) {
            throw new RuntimeException("Duplicate field: " + field.getId() + " on type " + getId());
        } else {
            this.fieldsMap.put(field.getId(), field);
            this.fields.add(field);
        }
    }

    @Override
    public Object getExtra() {
        return extra;
    }

    public void setExtra(Object extra) {
        this.extra = extra;
    }

    public void setDefaultViewId(String defaultViewId) {
        this.defaultViewId = defaultViewId;
    }

    public void setCreateViewId(String createViewId) {
        this.createViewId = createViewId;
    }
    
}
