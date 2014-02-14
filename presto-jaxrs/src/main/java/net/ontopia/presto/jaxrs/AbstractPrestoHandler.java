package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.utils.AbstractHandler;

public class AbstractPrestoHandler extends AbstractHandler {

    private Presto presto;

    public Presto getPresto() {
        return presto;
    }

    public void setPresto(Presto presto) {
        this.presto = presto;
    }

}
