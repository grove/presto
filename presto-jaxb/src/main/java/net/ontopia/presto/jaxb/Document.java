package net.ontopia.presto.jaxb;

public abstract class Document {

    public abstract String getFormat();

    public void setFormat(String format) {
        if (!getFormat().equals(format)) {
            throw new IllegalArgumentException("Invalid format: " + format + " Expected: " + getFormat());
        }
    }
    
}
