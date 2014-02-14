package net.ontopia.presto.jaxrs.constraints;

import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.utils.PrestoContext;

public class NotRemovableValueTypeConstraintException extends NotRemovableValueConstraintException {

    public NotRemovableValueTypeConstraintException(PrestoContext context, PrestoFieldUsage field, Object removableValue) {
        super(context, field, removableValue);
    }

    @Override
    protected String[] getMessageKeys() {
        return new String[] { "not-removable-value-type", "not-removable-value" };
    }

}
