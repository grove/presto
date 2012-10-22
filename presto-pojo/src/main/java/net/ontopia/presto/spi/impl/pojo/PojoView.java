package net.ontopia.presto.spi.impl.pojo;

import net.ontopia.presto.spi.PrestoView;

public class PojoView implements PrestoView {

    private String id;
    private ViewType type;
    private String name;

    private Object extra;

    PojoView(String id) {
        this.id = id;
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
    public ViewType getType() {
        return type;
    }

    public void setType(ViewType type) {
        this.type = type;
    }

    @Override
    public String getName() {
        return name;
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
