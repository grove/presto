package net.ontopia.presto.jaxrs.resolve;

import java.util.Collection;

import net.ontopia.presto.jaxrs.AbstractHandler;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoType;

public abstract class AvailableFieldCreateTypesResolver extends AbstractHandler {

    public abstract Collection<PrestoType> getAvailableFieldCreateTypes(PrestoContext context, PrestoFieldUsage field);

}
