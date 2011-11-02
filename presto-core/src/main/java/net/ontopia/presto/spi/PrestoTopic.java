package net.ontopia.presto.spi;

import java.util.List;

public interface PrestoTopic {

    String getId();

    String getName();

    String getTypeId();

    List<Object> getValues(PrestoField field);

    PagedValues getValues(PrestoField field, int offset, int limit);

    public static interface PagedValues {
        List<Object> getValues();
        Paging getPaging();
        int getTotal();
    }

    public static interface Paging {
        int getOffset();
        int getLimit();
    }

}
