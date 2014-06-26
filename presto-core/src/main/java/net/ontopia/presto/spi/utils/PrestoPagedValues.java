package net.ontopia.presto.spi.utils;

import java.util.List;

import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Projection;

public class PrestoPagedValues implements PagedValues {

    private final List<? extends Object> values;
    private final Projection projection;
    private final int total;

    public PrestoPagedValues(List<? extends Object> values, Projection projection, int total) {
        this.values = values;
        this.projection = projection;
        this.total = total;
    }

    public List<? extends Object> getValues() {
        return values;
    }

    public Projection getProjection() {
        return projection;
    }

    public int getTotal() {
        return total;
    }

}
