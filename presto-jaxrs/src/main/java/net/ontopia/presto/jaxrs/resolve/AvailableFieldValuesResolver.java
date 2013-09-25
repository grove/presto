package net.ontopia.presto.jaxrs.resolve;

import java.util.Collection;

import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoContext;

public abstract class AvailableFieldValuesResolver extends AbstractHandler {

    public abstract Collection<? extends Object> getAvailableFieldValues(PrestoContext context,PrestoFieldUsage field, String query);

}
