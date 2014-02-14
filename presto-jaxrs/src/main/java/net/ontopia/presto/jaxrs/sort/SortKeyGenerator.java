package net.ontopia.presto.jaxrs.sort;

import net.ontopia.presto.jaxrs.AbstractPrestoHandler;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.utils.PrestoContext;

public abstract class SortKeyGenerator extends AbstractPrestoHandler {

    public abstract String getSortKey(PrestoContext context, PrestoFieldUsage field, Object value);
    
}
