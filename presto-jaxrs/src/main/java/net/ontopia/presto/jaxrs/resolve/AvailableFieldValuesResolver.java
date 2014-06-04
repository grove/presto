package net.ontopia.presto.jaxrs.resolve;

import java.util.Collection;

import net.ontopia.presto.jaxrs.AbstractPrestoHandler;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.utils.PrestoContextRules;

public abstract class AvailableFieldValuesResolver extends AbstractPrestoHandler {

    /*
     * Get values that are valid field values.
     * 
     * @returns String, PrestoTopic or Value.
     */
    public abstract Collection<? extends Object> getAvailableFieldValues(PrestoContextRules rules, PrestoField field, String query);

}
