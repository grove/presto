package net.ontopia.presto.spi;

public interface PrestoView {

    enum ViewType { 

        NORMAL_VIEW("normal-view"), COLUMN_VIEW("column-view"), EXTERNAL_VIEW("external-view");
        
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
            return ViewType.NORMAL_VIEW;
        }
    };
    
    String getId();

    ViewType getType();

    String getName();

    Object getExtra();

}
