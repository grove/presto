package net.ontopia.presto.jaxrs.sort;

import net.ontopia.presto.jaxrs.AbstractHandler;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.spi.PrestoFieldUsage;

public abstract class SortKeyGenerator extends AbstractHandler {

    public abstract String getSortKey(PrestoContext context, PrestoFieldUsage field, Object value);
    
}
