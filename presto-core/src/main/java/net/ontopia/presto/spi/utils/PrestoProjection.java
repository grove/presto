package net.ontopia.presto.spi.utils;

import net.ontopia.presto.spi.PrestoTopic.Projection;

public class PrestoProjection implements Projection {

    public static final Projection FIRST_ELEMENT = new PrestoProjection(0, 1);
    public static final Projection FIRST_PAGE = new PrestoProjection(0, FieldValues.DEFAULT_LIMIT);

    public static final Projection UNPAGED_ASC = new PrestoProjection("asc");
    public static final Projection UNPAGED_DESC = new PrestoProjection("desc");
    
    private final int offset;
    private final int limit;
    private final String orderBy;

    public PrestoProjection(String orderBy) {
        this(0, Integer.MIN_VALUE, orderBy);
    }
    
    public PrestoProjection(int offset, int limit) {
        this(offset, limit, null);
    }
    
    public PrestoProjection(int offset, int limit, String orderBy) {
        this.offset = offset;
        this.limit = limit;
        this.orderBy = orderBy;
    }

    @Override
    public boolean isPaged() {
        return limit >= 0;
    }

    @Override
    public int getOffset() {
        return offset;
    }

    @Override
    public int getLimit() {
        return limit;
    }

    @Override
    public String getOrderBy() {
        return orderBy;
    }

    @Override
    public boolean isSorted() {
        return orderBy != null;
    }
    
}
