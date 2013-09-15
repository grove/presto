package net.ontopia.presto.spi.utils;

import net.ontopia.presto.spi.PrestoFieldUsage;

public class PrestoContextField {

    private final PrestoContext context;
    private final PrestoFieldUsage field;
    
    public PrestoContextField(PrestoContext context, PrestoFieldUsage field) {
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