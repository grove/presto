package net.ontopia.presto.spi.utils;

import java.util.Collection;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.PrestoType;

public interface PrestoFunction {

    List<Object> execute(PrestoContext context, Collection<? extends Object> objects, 
            PrestoType type, PrestoField field, Paging paging);

}