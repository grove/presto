package net.ontopia.presto.spi;

import java.util.List;

public interface PrestoTopic {

    boolean isInline();

    String getId();

    String getName();

    String getName(PrestoFieldUsage field);

    String getTypeId();

    boolean hasValue(PrestoField field);
    
    List<? extends Object> getValues(PrestoField field);

    PagedValues getValues(PrestoField field, int offset, int limit);

    public static interface PagedValues {
        List<? extends Object> getValues();
        Paging getPaging();
        int getTotal();
    }

    public static interface Paging {
        int getOffset();
        int getLimit();
    }

    List<? extends Object> getStoredValues(PrestoField field);

    PagedValues getStoredValues(PrestoField field, int offset, int limit);

}
