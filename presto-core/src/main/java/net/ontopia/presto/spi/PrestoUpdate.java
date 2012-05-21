package net.ontopia.presto.spi;

import java.util.Collection;

public interface PrestoUpdate {

    void setValues(PrestoField field, Collection<?> values);

    void addValues(PrestoField field, Collection<?> values);

    void addValues(PrestoField field, Collection<?> values, int index);

    void removeValues(PrestoField field, Collection<?> values);
    
//    boolean isNewTopic();
    
    boolean isFieldUpdated(PrestoField field);

    PrestoTopic getTopic();
    
    PrestoTopic getTopicAfterSave();
    
}
