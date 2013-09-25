package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoSchemaProvider;

public class NotInlineTypeConstraintException extends DefaultConstraintException {

    public NotInlineTypeConstraintException(PrestoSchemaProvider schemaProvider) {
        super(schemaProvider);
    }

    @Override
    protected String getMessageKey() {
        return "not-inline-type";
    }
    
}
