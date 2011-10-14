package net.ontopia.presto.spi.utils;

import java.util.Collection;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoType;

import org.codehaus.jackson.node.ObjectNode;

public interface PrestoFieldResolver {

    PagedValues resolve(Collection<? extends Object> objects,
            PrestoType type, PrestoField field, boolean isReference, ObjectNode resolveConfig, 
            boolean paging, int _limit, int offset, int limit);

}
