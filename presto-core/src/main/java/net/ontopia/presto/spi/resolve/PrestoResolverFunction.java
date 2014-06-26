package net.ontopia.presto.spi.resolve;

import java.util.Collection;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.utils.PrestoVariableContext;

import org.codehaus.jackson.node.ObjectNode;

public interface PrestoResolverFunction {

    List<Object> execute(PrestoVariableContext context, ObjectNode config, Collection<? extends Object> objects, 
            PrestoField field, Projection projection);

}
