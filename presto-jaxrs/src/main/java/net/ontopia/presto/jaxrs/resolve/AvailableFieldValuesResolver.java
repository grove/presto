package net.ontopia.presto.jaxrs.resolve;

import java.util.Collection;

import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.process.AbstractProcessor;
import net.ontopia.presto.spi.PrestoFieldUsage;

public abstract class AvailableFieldValuesResolver extends AbstractProcessor {

    public abstract Collection<? extends Object> getAvailableFieldValues(PrestoContext context,PrestoFieldUsage field);

}
