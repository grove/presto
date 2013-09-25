package net.ontopia.presto.spi;

import java.util.Collection;

public interface PrestoSchemaProvider {

    String getDatabaseId();

    PrestoType getTypeById(String typeId);

    PrestoType getTypeById(String typeId, PrestoType defaultValue);

    Collection<PrestoType> getRootTypes();

    Object getExtra();

}
