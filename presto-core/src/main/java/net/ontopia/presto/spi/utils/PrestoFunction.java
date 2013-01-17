package net.ontopia.presto.spi.utils;

import java.util.Collection;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.Paging;

public interface PrestoFunction {

    List<Object> execute(PrestoVariableContext context, Collection<? extends Object> objects, 
            PrestoField field, Paging paging);

}
