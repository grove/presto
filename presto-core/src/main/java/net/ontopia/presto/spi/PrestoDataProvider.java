package net.ontopia.presto.spi;

import java.util.Collection;

import net.ontopia.presto.spi.resolve.PrestoResolver;

public interface PrestoDataProvider {

    String getProviderId();
    
    PrestoTopic getTopicById(String topicId);

    Collection<PrestoTopic> getTopicsByIds(Collection<String> topicIds);

    Collection<? extends Object> getAvailableFieldValues(PrestoTopic topic, PrestoField field, String query);

    PrestoChangeSet newChangeSet();

    PrestoChangeSet newChangeSet(ChangeSetHandler handler);

    PrestoInlineTopicBuilder createInlineTopic(PrestoType type, String topicId);

    PrestoLazyTopicBuilder createLazyTopic(PrestoType type, String topicId);

    void close();

    public static interface ChangeSetHandler {

        public void onBeforeSave(PrestoChangeSet changeSet);

        public void onAfterSave(PrestoChangeSet changeSet);
        
    }

    PrestoResolver getResolver();
    
}
