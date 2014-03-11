package net.ontopia.presto.spi.utils;

import java.util.Collections;
import java.util.List;

public class FieldValues {
    
    public static final FieldValues EMPTY = new FieldValues(Collections.emptyList());
    public static final int DEFAULT_LIMIT = 100;
    
    private final List<? extends Object> values;
    private final boolean isPaging;
    private final int offset;
    private final int limit;
    private final int total;

    private FieldValues(List<? extends Object> values) {
        this.values = values;
        this.isPaging = false;
        this.offset = 0;
        this.limit = DEFAULT_LIMIT;
        this.total = values.size();
    }
    
    private FieldValues(List<? extends Object> values, int offset, int limit, int total) {
        this.values = values;
        this.isPaging = true;
        this.offset = offset;
        this.limit = limit;
        this.total = total;
    }

    public static FieldValues create (List<? extends Object> values) {
        if (values.isEmpty()) {
            return FieldValues.EMPTY;
        } else {
            return new FieldValues(values);
        }
    }

    public static FieldValues create (List<? extends Object> values, int offset, int limit, int total) {
        if (values.isEmpty()) {
            return FieldValues.EMPTY;
        } else {
            return new FieldValues(values, offset, limit, total);
        }
    }
    
    public List<? extends Object> getValues() {
        return values;
    }

    public boolean isPaging() {
        return isPaging;
    }
    
    public int getOffset() {
        return offset;
    }

    public int getLimit() {
        return limit;
    }

    public int getTotal() {
        return total;
    }
    
}