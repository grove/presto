package net.ontopia.presto.spi.utils;

import net.ontopia.presto.spi.PrestoTopic.Paging;

public class PrestoPaging implements Paging {

    private final int offset;
    private final int limit;

    public PrestoPaging(int offset, int limit) {
        this.offset = offset;
        this.limit = limit;
    }

    public int getOffset() {
        return offset;
    }

    public int getLimit() {
        return limit;
    }

}
