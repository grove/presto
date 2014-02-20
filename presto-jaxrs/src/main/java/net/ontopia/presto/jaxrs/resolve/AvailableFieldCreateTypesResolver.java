package net.ontopia.presto.jaxrs.resolve;

import java.util.Collection;

import net.ontopia.presto.jaxrs.AbstractPrestoHandler;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.PrestoContext;

public abstract class AvailableFieldCreateTypesResolver extends AbstractPrestoHandler {

    public abstract Collection<PrestoType> getAvailableFieldCreateTypes(PrestoContext context, PrestoField field);

}
