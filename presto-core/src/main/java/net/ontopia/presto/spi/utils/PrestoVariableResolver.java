package net.ontopia.presto.spi.utils;

import java.util.List;

public interface PrestoVariableResolver {

    List<String> getValues(Object value, String variable);
    
}
