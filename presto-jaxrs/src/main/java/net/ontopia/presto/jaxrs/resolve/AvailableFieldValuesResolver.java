package net.ontopia.presto.jaxrs.resolve;

import java.util.Collection;

import net.ontopia.presto.jaxrs.AbstractPrestoHandler;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.utils.PrestoContext;

public abstract class AvailableFieldValuesResolver extends AbstractPrestoHandler {

    public abstract Collection<? extends Object> getAvailableFieldValues(PrestoContext context,PrestoField field, String query);

}
