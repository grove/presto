package net.ontopia.presto.spi;

import java.util.Collection;

public interface PrestoDataProvider {

    String getProviderId();
    
    PrestoTopic getTopicById(String id);

    Collection<PrestoTopic> getTopicsByIds(Collection<String> id);

    Collection<PrestoTopic> getAvailableFieldValues(PrestoFieldUsage field);

    PrestoChangeSet newChangeSet();
    
    void close();

}
