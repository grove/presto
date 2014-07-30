package net.ontopia.presto.spi.functions;

import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoAttributes;
import net.ontopia.presto.spi.utils.PrestoContext;

public abstract class PrestoFieldFunction extends AbstractHandler {
    
    private PrestoAttributes attributes;

    public PrestoAttributes getAttributes() {
        return attributes;
    }

    public void setAttributes(PrestoAttributes attributes) {
        this.attributes = attributes;
    }
    
    public abstract List<? extends Object> execute(PrestoContext context, PrestoField field, Projection projection);

}
