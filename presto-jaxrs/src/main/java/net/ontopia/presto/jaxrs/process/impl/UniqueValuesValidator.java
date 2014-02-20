package net.ontopia.presto.jaxrs.process.impl;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.utils.PrestoContextRules;

public class UniqueValuesValidator extends IfThenElseResolveFieldDataProcessor {

    @Override
    protected boolean isShouldRun(FieldData fieldData, PrestoContextRules rules, PrestoField field) {
        return !rules.isReadOnlyField(field);
    }

    @Override
    public FieldData thenProcessFieldData(FieldData fieldData, PrestoContextRules rules, PrestoField field) {
        setValid(false);
        addError(fieldData, getErrorMessage("not-unique", field, "Field value is not unique."));
        return fieldData;
    }
    
}
