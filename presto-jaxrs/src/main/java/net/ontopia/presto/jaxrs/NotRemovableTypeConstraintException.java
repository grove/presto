package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoSchemaProvider;

public class NotRemovableTypeConstraintException extends DefaultConstraintException {

    public NotRemovableTypeConstraintException(PrestoSchemaProvider schemaProvider) {
        super(schemaProvider);
    }

    @Override
    protected String[] getMessageKeys() {
        return new String[] { "not-removable-type" };
    }
    
}
