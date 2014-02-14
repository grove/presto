package net.ontopia.presto.jaxrs.function;

import java.util.List;

import net.ontopia.presto.jaxrs.AbstractPrestoHandler;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.utils.PrestoContext;

public abstract class PrestoFieldFunction extends AbstractPrestoHandler {
    
    public abstract List<? extends Object> execute(PrestoContext context, PrestoFieldUsage field);

}
