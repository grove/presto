package net.ontopia.presto.jaxrs;


public abstract class ConstraintException extends RuntimeException {

    public ConstraintException() {
    }
    
    public ConstraintException(String message) {
        super(message);
    }
    
    public abstract String getType();
    
    public abstract String getTitle();
    
}
