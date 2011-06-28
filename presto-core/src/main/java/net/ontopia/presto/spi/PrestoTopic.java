package net.ontopia.presto.spi;

import java.util.List;

public interface PrestoTopic {

    String getId();

    String getName();

    String getTypeId();

    List<Object> getValues(PrestoField field);

    PagingValues getValues(PrestoField field, int offset, int limit);

    public static interface PagingValues {
    	List<Object> getValues();
    	int getOffset();
    	int getLimit();
    	int getTotal();
    }
    
}
