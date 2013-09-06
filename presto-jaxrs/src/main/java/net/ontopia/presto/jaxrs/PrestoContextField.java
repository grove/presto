package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoFieldUsage;

public class PrestoContextField {

    private final PrestoContext context;
    private final PrestoFieldUsage field;
    
    PrestoContextField(PrestoContext context, PrestoFieldUsage field) {
        this.context = context;
        this.field = field;
    }
    
    public PrestoContext getContext() {
        return context;
    }
    
    public PrestoFieldUsage getField() {
        return field;
    }
    
}