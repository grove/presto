package net.ontopia.presto.spi;

import java.util.Collection;

public interface PrestoUpdate {

    Collection<?> getValues(PrestoField field);

    void setValues(PrestoField field, Collection<?> values);

    void addValues(PrestoField field, Collection<?> values);

    void addValues(PrestoField field, Collection<?> values, int index);

    void removeValues(PrestoField field, Collection<?> values);
    
    boolean isNewTopic();
    
    boolean isTopicUpdated();
    
    boolean isFieldUpdated(PrestoField field);
    
    PrestoTopic getTopicAfterSave();
    
}
