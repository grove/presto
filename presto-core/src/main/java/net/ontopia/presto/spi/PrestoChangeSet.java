package net.ontopia.presto.spi;

import java.util.Collection;

public interface PrestoChangeSet {

  void setValues(PrestoFieldUsage field, Collection<Object> values);

  void addValues(PrestoFieldUsage field, Collection<Object> values);

  void addValues(PrestoFieldUsage field, Collection<Object> values, int index);

  void removeValues(PrestoFieldUsage field, Collection<Object> values);

  PrestoTopic save();
  
}
