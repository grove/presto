package net.ontopia.presto.spi.utils;

import java.util.List;

public interface PrestoVariableResolver {

    List<? extends Object> getValues(Object value, String variable);
    
}
