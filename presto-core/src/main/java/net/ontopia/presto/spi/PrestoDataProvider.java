package net.ontopia.presto.spi;

import java.util.Collection;

public interface PrestoDataProvider {

    String getProviderId();
    
    PrestoTopic getTopicById(String id);

    Collection<PrestoTopic> getTopicsByIds(Collection<String> id);

    Collection<? extends Object> getAvailableFieldValues(PrestoTopic topic, PrestoFieldUsage field);

    PrestoChangeSet newChangeSet(ChangeSetHandler handler);
    
    void close();

    public static interface ChangeSetHandler {

        public void onBeforeSave(PrestoChangeSet changeSet, PrestoChanges changes);

        public void onAfterSave(PrestoChangeSet changeSet, PrestoChanges changes);
        
    }
    
}
