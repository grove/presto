package net.ontopia.presto.spi;

import java.util.Collection;
import java.util.List;

import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

import org.codehaus.jackson.JsonNode;

public interface PrestoDataProvider {

    String getProviderId();
    
    PrestoTopic getTopicById(String topicId);

    Collection<PrestoTopic> getTopicsByIds(Collection<String> topicIds);

    Collection<? extends Object> getAvailableFieldValues(PrestoTopic topic, PrestoFieldUsage field, String query);

    PrestoChangeSet newChangeSet();

    PrestoChangeSet newChangeSet(ChangeSetHandler handler);

    PrestoInlineTopicBuilder createInlineTopic(PrestoType type, String topicId);

    void close();

    public static interface ChangeSetHandler {

        public void onBeforeSave(PrestoChangeSet changeSet, PrestoChanges changes);

        public void onAfterSave(PrestoChangeSet changeSet, PrestoChanges changes);
        
    }

    List<? extends Object> resolveValues(PrestoTopic topic, PrestoField field);

    PagedValues resolveValues(PrestoTopic topic, PrestoField field, int offset, int limit);

    PagedValues resolveValues(Collection<? extends Object> objects,
            PrestoField field, Paging paging, JsonNode resolveConfig, PrestoVariableResolver variableResolver);
    
}
