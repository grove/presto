package net.ontopia.presto.spi.utils;

import java.util.List;

import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;

public class PrestoPagedValues implements PagedValues {

    private final List<Object> values;
    private final Paging paging;
    private final int total;

    public PrestoPagedValues(List<Object> values, Paging paging, int total) {
        this.values = values;
        this.paging = paging;
        this.total = total;
    }

    public List<Object> getValues() {
        return values;
    }

    public Paging getPaging() {
        return paging;
    }

    public int getTotal() {
        return total;
    }

}
