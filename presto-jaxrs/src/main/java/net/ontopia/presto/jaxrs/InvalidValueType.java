package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoContext;

public class InvalidValueType extends ConstraintException {
    
    private static final String MESSAGE_KEY = "invalid-value-type";
    
    @SuppressWarnings("unused")
    private final PrestoContext context;
    private final PrestoFieldUsage field;
    @SuppressWarnings("unused")
    private final PrestoTopic value;

    public InvalidValueType(PrestoContext context, PrestoFieldUsage field, PrestoTopic value) {
        this.context = context;
        this.field = field;
        this.value = value;
    }

    private PrestoSchemaProvider getSchemaProvider() {
        return field.getSchemaProvider();
    }
    
    @Override
    public String getType() {
        String type = getType(getSchemaProvider(), MESSAGE_KEY);
        return type != null ? type : "ALERT";
    }
    
    @Override
    public String getTitle() {
        String title = getTitle(getSchemaProvider(), MESSAGE_KEY);
        return title != null ? title : "Constraint error";
    }
    
    @Override
    public String getMessage() {
        String message = getMessage(getSchemaProvider(), MESSAGE_KEY);
        return message != null ? message : "Value of invalid type.";
    }

}
