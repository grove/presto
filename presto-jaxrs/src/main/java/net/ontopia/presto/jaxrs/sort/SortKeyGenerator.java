package net.ontopia.presto.jaxrs.sort;

import net.ontopia.presto.jaxrs.AbstractHandler;
import net.ontopia.presto.spi.PrestoFieldUsage;

public abstract class SortKeyGenerator extends AbstractHandler {

    public abstract String getSortKey(PrestoFieldUsage field, Object value);
    
}
