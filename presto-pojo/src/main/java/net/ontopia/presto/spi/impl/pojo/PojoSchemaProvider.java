package net.ontopia.presto.spi.impl.pojo;

import java.util.HashMap;
import java.util.Map;

import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;

public class PojoSchemaProvider implements PrestoSchemaProvider {

    private String databaseId;

    private Map<String,PrestoType> typesMap = new HashMap<String,PrestoType>();

    private Object extra;

    public static PojoSchemaProvider getSchemaProvider(String databaseId, String schemaFile) {
        PojoSchemaProvider schemaProvider = PojoSchemaModel.parse(databaseId, schemaFile);
        schemaProvider.sanityCheck();
        return schemaProvider;
    }

    private void sanityCheck() {
        for (PrestoType type : typesMap.values()) {
            sanityCheck(type);
        }
    }

    private void sanityCheck(PrestoType type) {
        for (PrestoView view : type.getViews(null)) {
            for (PrestoFieldUsage field : type.getFields(view)) {
                if (field.isCascadingDelete()) {
                    for (PrestoType valueType : field.getAvailableFieldValueTypes()) {
                        if (!valueType.isRemovableCascadingDelete()) {
                            throw new SchemaException("Value type with removableCascadingDelete=false (" + valueType + ") in field with cascadingDelete=true (" + field + ").");                            
                        }
                    }
                }
            }
        }
    }

    @Override
    public String getDatabaseId() {
        return databaseId;
    }

    @Override
    public PrestoType getTypeById(String typeId) {
        PrestoType type = typesMap.get(typeId);
        if (type == null) {
            throw new RuntimeException("Unknown type: " + typeId);
        }
        return type;
    }
    
    @Override
    public PrestoType getTypeById(String typeId, PrestoType defaultValue) {
        PrestoType type = typesMap.get(typeId);
        if (type == null) {
            return defaultValue;
        }
        return type;
    }

    protected void setDatabaseId(String databaseId) {
        this.databaseId = databaseId;
    }

    protected void addType(PrestoType type) {
        this.typesMap.put(type.getId(), type);        
    }

    @Override
    public Object getExtra() {
        return extra;
    }

    public void setExtra(Object extra) {
        this.extra = extra;
    }

}
