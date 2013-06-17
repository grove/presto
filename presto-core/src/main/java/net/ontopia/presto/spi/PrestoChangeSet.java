package net.ontopia.presto.spi;

public interface PrestoChangeSet {

    PrestoUpdate createTopic(PrestoType type);

    PrestoUpdate createTopic(PrestoType type, String topicId);

    PrestoUpdate updateTopic(PrestoTopic topic, PrestoType type);

    void deleteTopic(PrestoTopic topic, PrestoType type);

    void deleteTopic(PrestoTopic topic, PrestoType type, PrestoField field);

    void save();

}
