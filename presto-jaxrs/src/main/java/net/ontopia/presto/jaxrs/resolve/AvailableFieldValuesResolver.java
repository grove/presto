package net.ontopia.presto.jaxrs.resolve;

import java.util.Collection;

import net.ontopia.presto.jaxrs.AbstractHandler;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.spi.PrestoFieldUsage;

public abstract class AvailableFieldValuesResolver extends AbstractHandler {

    public abstract Collection<? extends Object> getAvailableFieldValues(PrestoContext context,PrestoFieldUsage field, String query);

}
