package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoSchemaProvider;

public class InvalidValueTypeConstraintException extends DefaultConstraintException {
    
    public InvalidValueTypeConstraintException(PrestoSchemaProvider schemaProvider) {
        super(schemaProvider);
    }

    @Override
    protected String getMessageKey() {
        return "invalid-value-type";
    }

}
