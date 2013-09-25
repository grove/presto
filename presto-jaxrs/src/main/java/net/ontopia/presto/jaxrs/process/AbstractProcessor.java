package net.ontopia.presto.jaxrs.process;

import java.util.ArrayList;
import java.util.Collection;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.jaxrs.PrestoProcessor.Status;
import net.ontopia.presto.jaxrs.PrestoProcessor.Type;
import net.ontopia.presto.spi.utils.AbstractHandler;

import org.codehaus.jackson.JsonNode;

public class AbstractProcessor extends AbstractHandler {

    private Presto presto;
    
    private Status status;
    private Type processType;

    public Presto getPresto() {
        return presto;
    }

    public void setPresto(Presto presto) {
        this.presto = presto;
    }

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

    // -- statics
    
    public static <T extends AbstractProcessor> Iterable<T> getProcessors(Presto presto, Class<T> klass, JsonNode processorsNode) {
        Iterable<T> handlers = AbstractHandler.getHandlers(presto.getDataProvider(), presto.getSchemaProvider(), klass, processorsNode);
        for (T handler : handlers) {
            handler.setPresto(presto);
        }
        return handlers;
    }
    
    public static <T extends AbstractProcessor> T getProcessor(Presto presto, Class<T> klass, JsonNode processorNode) {
        T handler = AbstractHandler.getHandler(presto.getDataProvider(), presto.getSchemaProvider(), klass, processorNode);
        if (handler != null) {
            handler.setPresto(presto);
        }
        return handler;
    }
    
}
