package net.ontopia.presto.jaxrs.sort;

import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoContext;

public abstract class SortKeyGenerator extends AbstractHandler {

    public abstract String getSortKey(PrestoContext context, PrestoFieldUsage field, Object value);
    
}
