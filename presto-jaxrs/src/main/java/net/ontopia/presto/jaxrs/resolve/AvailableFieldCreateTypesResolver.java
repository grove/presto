package net.ontopia.presto.jaxrs.resolve;

import java.util.Collection;

import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoContext;

public abstract class AvailableFieldCreateTypesResolver extends AbstractHandler {

    public abstract Collection<PrestoType> getAvailableFieldCreateTypes(PrestoContext context, PrestoFieldUsage field);

}
