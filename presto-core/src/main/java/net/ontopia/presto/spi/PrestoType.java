package net.ontopia.presto.spi;

import java.util.Collection;
import java.util.List;

public interface PrestoType {

    String getId();

    String getName();

    PrestoSchemaProvider getSchemaProvider();

    boolean isInline();

    boolean isHidden(); // will it show up? instances will show up if exposed as field values though.

    boolean isCreatable(); // standalone creatable. default is yes. may still be created through a field if in createTypes.

    boolean isRemovable();

    boolean isRemovableCascadingDelete();
    
    // TODO: getSuperType();

    @Deprecated
    Collection<PrestoType> getDirectSubTypes();

    List<PrestoField> getFields();

    List<PrestoFieldUsage> getFields(PrestoView fieldsView);

    PrestoField getFieldById(String fieldId);

    PrestoFieldUsage getFieldById(String fieldId, PrestoView view);

    PrestoView getDefaultView();

    PrestoView getCreateView();
    
    PrestoView getViewById(String viewId);

    Collection<PrestoView> getViews(PrestoView fieldsView);

    Object getExtra();

}
