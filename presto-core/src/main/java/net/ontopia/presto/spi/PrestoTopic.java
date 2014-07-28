package net.ontopia.presto.spi;

import java.util.List;

public interface PrestoTopic {

    boolean isInline();

    String getId();

    String getName();

    String getName(PrestoField field);

    String getTypeId();

    public interface PagedValues {
        Projection getProjection();
        List<? extends Object> getValues();
        int getTotal();
    }

    public interface Projection {
        boolean isPaged();
        boolean isSorted();
        int getOffset();
        int getLimit();
        String getOrderBy();
    }

    boolean hasValue(PrestoField field);
    
    List<? extends Object> getValues(PrestoField field);

    PagedValues getValues(PrestoField field, Projection projection);
    
    List<? extends Object> getStoredValues(PrestoField field);
    
    PagedValues getStoredValues(PrestoField field, Projection projection);

    Object getInternalData(); // WARN: be careful, no guarantees given

}
