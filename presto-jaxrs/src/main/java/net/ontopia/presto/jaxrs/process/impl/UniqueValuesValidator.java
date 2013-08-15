package net.ontopia.presto.jaxrs.process.impl;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.spi.PrestoFieldUsage;

public class UniqueValuesValidator extends IfThenElseResolveProcessor {
    
    @Override
    public FieldData thenProcessFieldData(FieldData fieldData,  PrestoContext context, PrestoFieldUsage field) {
        setValid(false);
        addError(fieldData, getErrorMessage("not-unique", field, "Field value is not unique."));
        return fieldData;
    }
    
}
