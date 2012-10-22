package net.ontopia.presto.spi;

public interface PrestoView {

    enum ViewType { 

        EDIT_IN_VIEW("edit-in-view"), COLUMN_VIEW("column-view");
        
        private String typeId;
        private ViewType(String typeId) {
            this.typeId = typeId;
        }
        
        public String getLinkId() {
            return typeId;
        }
        
        public static ViewType findByTypeId(String typeId) {
            for (ViewType vt : values()) {
                if (typeId.equals(vt.typeId)) {
                    return vt;
                }
            }
            return ViewType.EDIT_IN_VIEW;
        }
    };
    
    String getId();

    ViewType getType();

    String getName();

    Object getExtra();

}
