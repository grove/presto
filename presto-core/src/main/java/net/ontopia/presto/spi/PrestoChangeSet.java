package net.ontopia.presto.spi;

import java.util.Collection;

public interface PrestoChangeSet {

  void setValues(PrestoField field, Collection<?> values);

  void addValues(PrestoField field, Collection<?> values);

  void addValues(PrestoField field, Collection<?> values, int index);

  void removeValues(PrestoField field, Collection<?> values);

  PrestoTopic save();
  
}
