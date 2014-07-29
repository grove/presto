package net.ontopia.presto.jaxrs.process.impl;

import java.util.Collection;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.utils.PrestoContextRules;

public class FieldCardinalityValidator extends FieldDataProcessor {

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoContextRules rules, PrestoField field, Projection projection) {
        int minCardinality = field.getMinCardinality();
        int maxCardinality = field.getMaxCardinality();
        
        Collection<Value> values = fieldData.getValues();
        int cardinality = values.size();
        if (minCardinality > 0 && maxCardinality > 0) {
            if (cardinality < minCardinality || cardinality > maxCardinality) {
                setValid(false);
                if (minCardinality == maxCardinality) {
                    addError(fieldData, getErrorMessage("cardinality-exact", field, "Field must have exactly {0,choice,0#{0} values|1#{0} value|2#{0} values}", minCardinality, maxCardinality));
                } else {
                    addError(fieldData, getErrorMessage("cardinality-between", field, "Field must have between {0} and {1} values", minCardinality, maxCardinality));
                }
            }
        } else {
            if (minCardinality > 0 && cardinality < minCardinality) {
                setValid(false);
                addError(fieldData, getErrorMessage("cardinality-at-least", field, "Field must have at least {0,choice,0#{0} values|1#{0} value|2#{0} values}", minCardinality, maxCardinality));
            }
            if (maxCardinality > 0 && cardinality > maxCardinality) {
                setValid(false);
                addError(fieldData, getErrorMessage("cardinality-no-more-than", field, "Field must have no more than {1,choice,0#{1} values|1#{1} value|2#{1} values}", minCardinality, maxCardinality));
            }
        }
        return fieldData;
    }

}
