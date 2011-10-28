package net.ontopia.presto.spi.impl.pojo;

import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoView;

public class PojoView implements PrestoView {

    private String id;
    private String name;
    private PrestoSchemaProvider schemaProvider;

    private Object extra;

    PojoView(String id, PrestoSchemaProvider schemaProvider) {
        this.id = id;
        this.schemaProvider = schemaProvider;        
    }

    @Override
    public String toString() {
        return "PojoView[" + getId() + "|" + getName() + "]";
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof PojoView) {
            PojoView o = (PojoView)other;
            return id.equals(o.id);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public PrestoSchemaProvider getSchemaProvider() {
        return schemaProvider;
    }

    protected void setName(String name) {
        this.name = name;
    }

    @Override
    public Object getExtra() {
        return extra;
    }

    public void setExtra(Object extra) {
        this.extra = extra;
    }

}
