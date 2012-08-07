package net.ontopia.presto.spi;

import java.util.Collection;

public interface PrestoChanges {
    
    Collection<? extends PrestoUpdate> getCreated();
    
    Collection<? extends PrestoUpdate> getUpdated();

    Collection<? extends PrestoTopic> getDeleted();


}
