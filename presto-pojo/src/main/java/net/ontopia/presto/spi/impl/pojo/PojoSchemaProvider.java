package net.ontopia.presto.spi.impl.pojo;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoType;

public class PojoSchemaProvider implements PrestoSchemaProvider {

    private String databaseId;

    private Map<String,PrestoType> typesMap = new HashMap<String,PrestoType>();

    private Object extra;

    public static PojoSchemaProvider getSchemaProvider(String databaseId, String schemaFile) {
        return PojoSchemaModel.parse(databaseId, schemaFile);
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
    public Collection<PrestoType> getRootTypes() {
        Collection<PrestoType> result = new HashSet<PrestoType>(typesMap.values());
        Set<PrestoType> notSuperTypes = new HashSet<PrestoType>();
        for (PrestoType type : result) {
            notSuperTypes.addAll(type.getDirectSubTypes());
        }
        result.removeAll(notSuperTypes);
        return result;
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
