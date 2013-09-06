package net.ontopia.presto.jaxrs.process.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.PrestoContextRules;
import net.ontopia.presto.spi.PrestoFieldUsage;

public class ReadOnlyIfPostProcessor extends IfThenElseResolveFieldDataProcessor {
    
    private static final Set<String> mutableLinkRels = new HashSet<String>() {{
        add("add-field-values");
        add("add-field-values-at-index");
        add("remove-field-values");
        add("move-field-values-to-index");
        add("available-field-values");
        add("create-field-instance");
    }};
    
    @Override
    public FieldData thenProcessFieldData(FieldData fieldData, PrestoContextRules rules, PrestoFieldUsage field) {
        return makeFieldReadOnly(fieldData);        
    }

    private FieldData makeFieldReadOnly(FieldData fieldData) {
        if (isValid(fieldData)) {
            fieldData.setReadOnly(true);
    
            // Remove mutable links
            removeLinksByRel(fieldData, mutableLinkRels);
            
            // Remove Value.removable=true
            clearRemovableValues(fieldData.getValues());
            
            // ISSUE: remove certain messages?
        }
        return fieldData;
    }

    private boolean isValid(FieldData fieldData) {
        Collection<String> errors = fieldData.getErrors();
        return errors == null || errors.isEmpty();
    }

    private void clearRemovableValues(Collection<Value> values) {
        for (Value v : values) {
            v.setRemovable(null);
        }
    }

    private void removeLinksByRel(FieldData fieldData, Set<String> linkRels) {
        Collection<Link> links = fieldData.getLinks();
        if (links != null) {
            Iterator<Link> iter = links.iterator();
            while (iter.hasNext()) {
                Link link = iter.next();
                String rel = link.getRel();
                if (linkRels.contains(rel)) {
                    iter.remove();
                }
            }
        }
    }

}
