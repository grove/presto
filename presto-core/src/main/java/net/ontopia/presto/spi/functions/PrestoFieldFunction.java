package net.ontopia.presto.spi.functions;

import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoContext;

public abstract class PrestoFieldFunction extends AbstractHandler {
    
    public abstract List<? extends Object> execute(PrestoContext context, PrestoField field);

}
