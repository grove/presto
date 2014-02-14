package net.ontopia.presto.jaxrs.constraints;

import net.ontopia.presto.spi.PrestoSchemaProvider;

public class InvalidValueTypeConstraintException extends DefaultConstraintException {
    
    public InvalidValueTypeConstraintException(PrestoSchemaProvider schemaProvider) {
        super(schemaProvider);
    }

    @Override
    protected String[] getMessageKeys() {
        return new String[] { "invalid-value-type" };
    }

}
