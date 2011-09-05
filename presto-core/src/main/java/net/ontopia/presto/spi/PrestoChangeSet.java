package net.ontopia.presto.spi;

public interface PrestoChangeSet {

    PrestoUpdate createTopic(PrestoType type);

    PrestoUpdate updateTopic(PrestoTopic topic, PrestoType type);

    void deleteTopic(PrestoTopic topic, PrestoType type);

    void save();
    
}
