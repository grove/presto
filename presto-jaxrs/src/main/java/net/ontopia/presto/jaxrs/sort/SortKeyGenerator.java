package net.ontopia.presto.jaxrs.sort;

import java.util.Comparator;

import net.ontopia.presto.jaxrs.AbstractPrestoHandler;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.utils.PrestoContextRules;

public abstract class SortKeyGenerator extends AbstractPrestoHandler {
    
    public abstract Comparator<Object> getComparator(PrestoContextRules rules, PrestoField field, Projection projection);

}
