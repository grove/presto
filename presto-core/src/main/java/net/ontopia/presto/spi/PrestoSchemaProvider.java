package net.ontopia.presto.spi;

public interface PrestoSchemaProvider {

    String getDatabaseId();

    PrestoType getTypeById(String typeId);

    PrestoType getTypeById(String typeId, PrestoType defaultValue);

    Object getExtra();

}
