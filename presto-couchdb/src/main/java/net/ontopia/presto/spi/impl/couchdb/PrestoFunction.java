package net.ontopia.presto.spi.impl.couchdb;

import java.util.Collection;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoType;

public interface PrestoFunction {

    List<Object> execute(PrestoContext context, Collection<? extends Object> objects, 
            PrestoType type, PrestoField field, boolean paging, int _limit, int offset, int limit);

}
