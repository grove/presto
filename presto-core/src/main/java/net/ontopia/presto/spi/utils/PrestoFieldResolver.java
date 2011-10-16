package net.ontopia.presto.spi.utils;

import java.util.Collection;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.PrestoType;

public interface PrestoFieldResolver {

    PagedValues resolve(Collection<? extends Object> objects,
            PrestoType type, PrestoField field, boolean isReference, Paging paging);

}
