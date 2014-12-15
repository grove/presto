package net.ontopia.presto.spi;

import java.util.Collection;

public interface PrestoLazyTopicBuilder {

    public void setValues(PrestoField field, Collection<?> values);
    
    public PrestoTopic build();

}
