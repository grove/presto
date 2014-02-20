package net.ontopia.presto.spi.utils;

import net.ontopia.presto.spi.PrestoField;

public class PrestoContextField {

    private final PrestoContext context;
    private final PrestoField field;
    
    public PrestoContextField(PrestoContext context, PrestoField field) {
        this.context = context;
        this.field = field;
    }
    
    public PrestoContext getContext() {
        return context;
    }
    
    public PrestoField getField() {
        return field;
    }
    
}