package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoSchemaProvider;

public class NotInlineTypeConstraintException extends DefaultConstraintException {

    public NotInlineTypeConstraintException(PrestoSchemaProvider schemaProvider) {
        super(schemaProvider);
    }

    @Override
    protected String[] getMessageKeys() {
        return new String[] { "not-inline-type" };
    }
    
}
