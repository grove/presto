package net.ontopia.presto.jaxrs.process;

import java.util.ArrayList;
import java.util.Collection;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxrs.AbstractHandler;
import net.ontopia.presto.jaxrs.PrestoProcessor.Status;
import net.ontopia.presto.jaxrs.PrestoProcessor.Type;

public class AbstractProcessor extends AbstractHandler {

    private Status status;
    private Type processType;
    
    public Type getType() {
        return processType;
    }
    
    public void setType(Type processType) {
        this.processType = processType;
    }

    protected boolean isPreProcess() {
        return processType == Type.PRE_PROCESS;
    }

    protected boolean isPostProcess() {
        return processType == Type.POST_PROCESS;
    }

    public Status getStatus() {
        return status;
    }
    
    public void setStatus(Status status) {
        this.status = status;
    }
    
    protected void setValid(boolean isValid) {
        if (status != null) {
            status.setValid(isValid);
        }
    }
    
    protected void addError(FieldData fieldData, String error) {
        Collection<String> errors = fieldData.getErrors();
        if (errors == null) {
            errors = new ArrayList<String>();
            fieldData.setErrors(errors);
        }
        errors.add(error);
    }
   
}
