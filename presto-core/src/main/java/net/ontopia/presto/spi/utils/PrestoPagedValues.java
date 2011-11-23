package net.ontopia.presto.spi.utils;

import java.util.List;

import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;

public class PrestoPagedValues implements PagedValues {

    private final List<? extends Object> values;
    private final Paging paging;
    private final int total;

    public PrestoPagedValues(List<? extends Object> values, Paging paging, int total) {
        this.values = values;
        this.paging = paging;
        this.total = total;
    }

    public List<? extends Object> getValues() {
        return values;
    }

    public Paging getPaging() {
        return paging;
    }

    public int getTotal() {
        return total;
    }

}
