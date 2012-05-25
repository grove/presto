package net.ontopia.presto.spi;

import java.util.Collection;

public interface PrestoChanges {
    
    PrestoUpdate getUpdate(PrestoTopic topic);
    
    Collection<? extends PrestoUpdate> getUpdates();

    Collection<? extends PrestoTopic> getDeleted();

}
