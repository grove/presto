package net.ontopia.presto.spi.impl.pojo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView.ViewType;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PojoSchemaModel {

    private static Logger log = LoggerFactory.getLogger(PojoSchemaModel.class);

    static PojoSchemaProvider parse(String databaseId, String schemaFilename) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream istream = null;
        try {
            File schemaFile = new File(System.getProperty("user.home") + File.separator + schemaFilename);
            if (schemaFile.exists()) {
                log.warn("Loading presto schema model from file in user's home directory: " + schemaFile.getAbsolutePath());
                istream = new FileInputStream(schemaFile);
            } else {
                istream = cl.getResourceAsStream(schemaFilename);
            }
            if (istream == null) {
                throw new RuntimeException("Cannot find schema file: " + schemaFilename);
            }
            Reader reader = new InputStreamReader(istream, "UTF-8");
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode objectNode = mapper.readValue(reader, ObjectNode.class);
            
            return createSchemaProvider(databaseId, objectNode);
            
        } catch (Exception e) {
            throw new RuntimeException("Problems occured when loading '" + schemaFilename + "'", e);
        } finally {
            try {
                if (istream != null) istream.close();
            } catch (IOException e) {
            }
        }
    }
    
    private static PojoSchemaProvider createSchemaProvider(String databaseId, ObjectNode json) {
        PojoSchemaParser parser = new PojoSchemaParser();
        parser.parse(databaseId, json);
        return parser.getSchemaProvider();
    }

    private static class PojoSchemaParser {

        private PojoSchemaProvider schemaProvider;
        private Map<String, ObjectNode> fieldsMap;
        private Map<String, ObjectNode> viewsMap;
        private Map<String, ObjectNode> typesMap;
        private Map<String,PojoType> types;
        
        public void parse(String databaseId, ObjectNode schema) {
            schemaProvider = new PojoSchemaProvider();
            schemaProvider.setDatabaseId(databaseId);

            // extra
            if (schema.has("extra")) {
                schemaProvider.setExtra(schema.get("extra"));
            }

            boolean defaultsReadOnly = false;
            JsonNode defaultsReadOnlyNode = schema.path("defaults").path("readOnly");
            if (defaultsReadOnlyNode.isBoolean()) {
                defaultsReadOnly = defaultsReadOnlyNode.getBooleanValue();
            } else if (!defaultsReadOnlyNode.isMissingNode()) {
                throw new RuntimeException("defaults.readOnly must be a boolean: " + defaultsReadOnlyNode);
            }
            
            fieldsMap = createFieldsMap(schema);
            viewsMap = createViewsMap(schema);
            typesMap = createTypesMap(schema);

            types = new HashMap<String,PojoType>();
            
            for (String typeId : typesMap.keySet()) {
                createTypes(typeId, defaultsReadOnly);            
            }
        }

        private void createTypes(String typeId, boolean readOnlyDefault) {
            ObjectNode typeConfig = typesMap.get(typeId);

            PojoType type = getPojoType(typeId, types, schemaProvider);
            schemaProvider.addType(type);

            // extra
            if (typeConfig.has("extra")) {
                type.setExtra(typeConfig.get("extra"));
            }

            // name
            String name = typeConfig.get("name").getTextValue();
            type.setName(name);

            // readOnly
            if (typeConfig.has("readOnly")) {
                readOnlyDefault = typeConfig.get("readOnly").getBooleanValue();
            }
            
            // inline
            if (typeConfig.has("inline")) {
                type.setInline(typeConfig.get("inline").getBooleanValue());
            }
            // hidden
            if (typeConfig.has("hidden")) {
                type.setHidden(typeConfig.get("hidden").getBooleanValue());
            }
            // creatable
            if (typeConfig.has("creatable")) {
                type.setCreatable(typeConfig.get("creatable").getBooleanValue());
            }
            // removable
            if (typeConfig.has("removable")) {
                type.setRemovable(typeConfig.get("removable").getBooleanValue());
            }
            // removableCascadingDelete
            if (typeConfig.has("removableCascadingDelete")) {
                type.setRemovableCascadingDelete(typeConfig.get("removableCascadingDelete").getBooleanValue());
            }

            // extends
            if (typeConfig.has("extends")) {
                String superTypeId = typeConfig.get("extends").getTextValue();
                verifyDeclaredType(superTypeId, typesMap, "extends", type);
                PojoType superType = getPojoType(superTypeId, types, schemaProvider);
                //                type.setSuperType(superType);
                superType.addDirectSubType(type);
            }
            // defaultView
            String defaultViewId;
            if (typeConfig.has("defaultView")) {
                defaultViewId = typeConfig.get("defaultView").getTextValue();
                type.setDefaultViewId(defaultViewId);
            } else {
                defaultViewId = "info"; // TODO: should 'info' or the first view be default?
                type.setDefaultViewId(defaultViewId);
            } 
            // createView
            if (typeConfig.has("createView")) {
                String createViewId = typeConfig.get("createView").getTextValue();
                type.setCreateViewId(createViewId);
            } else {
                type.setCreateViewId(defaultViewId);
            } 

            if (typeConfig.has("views")) {
                createViews(typeConfig, type, readOnlyDefault);
            }
            types.put(typeId, type);
        }

        private void createViews(ObjectNode typeConfig, PojoType type, boolean readOnlyDefault) {
            ArrayNode viewsNode = (ArrayNode)typeConfig.get("views");
            for (JsonNode viewNode : viewsNode) {
                String viewId;
                ObjectNode viewConfig;

                // view
                if (viewNode.isTextual()) {
                    viewId = viewNode.getTextValue();                        
                    viewConfig = viewsMap.get(viewId);
                } else if (viewNode.isObject()) {
                    viewConfig = (ObjectNode)viewNode;
                    viewId = viewConfig.get("id").getTextValue();
                } else {
                    throw new RuntimeException("Invalid view declaration or view reference: " + viewNode);
                }
               
                PojoView view = new PojoView(viewId);
                type.addView(view);

                // name
                String viewName = viewConfig.get("name").getTextValue();
                view.setName(viewName);

                // readOnly
                if (viewConfig.has("readOnly")) {
                    readOnlyDefault = viewConfig.get("readOnly").getBooleanValue();
                }

                // type
                ViewType viewType = ViewType.EDIT_IN_VIEW;
                if (viewConfig.has("type")) {
                    viewType = ViewType.findByTypeId(viewConfig.get("type").getTextValue());
                }
                view.setType(viewType);
                
                // extra
                if (viewConfig.has("extra")) {
                    view.setExtra(viewConfig.get("extra"));
                }

                // fields
                createFields(type, viewConfig, readOnlyDefault, view);
            }
        }

        private void createFields(PojoType type, ObjectNode viewConfig, boolean readOnlyDefault, PojoView view) {
            ArrayNode fieldsArray = (ArrayNode)viewConfig.get("fields");
            for (JsonNode fieldNode : fieldsArray) {
                PojoField field = createField(type, view, readOnlyDefault, fieldNode);
                type.addField(field);
            }
        }

        private PojoField createField(PojoType type, PojoView view, boolean readOnlyDefault, JsonNode fieldNode) {
            String fieldId;
            ObjectNode fieldConfig;
            if (fieldNode.isTextual()) {
                fieldId = fieldNode.getTextValue();                        
                fieldConfig = fieldsMap.get(fieldId);
            } else if (fieldNode.isObject()) {
                fieldConfig = (ObjectNode)fieldNode;
                fieldId = fieldConfig.get("id").getTextValue();
            } else {
                throw new RuntimeException("Invalid field declaration or field reference: " + fieldNode);
            }
            if (fieldId == null) {
                throw new RuntimeException("Field id missing on field object: " + fieldConfig);
            }
            if (fieldConfig == null) {
                throw new RuntimeException("Field declaration missing for field with id '" + fieldId + "'");
            }
            return createField(type, view, readOnlyDefault, fieldId, fieldConfig);
        }
        
        private PojoField createField(PojoType type, PojoView view, boolean readOnlyDefault, String fieldId, ObjectNode fieldConfig) {
            PojoField field = new PojoField(fieldId, schemaProvider);
            field.addDefinedInView(view);
            
            // actualId
            if (fieldConfig.has("actualId")) {
                field.setActualId(fieldConfig.get("actualId").getTextValue());
            }

            // name
            if (fieldConfig.has("name")) {
                String fieldName = fieldConfig.get("name").getTextValue();                        
                field.setName(fieldName);
            } else {
                field.setName(fieldId);
            }
            // isNameField
            if (fieldConfig.has("nameField")) {
                field.setNameField(fieldConfig.get("nameField").getBooleanValue());
            }

            // isPrimitiveField/isReferenceField

            // dataType
            if (fieldConfig.has("datatype")) {
                String datatype = fieldConfig.get("datatype").getTextValue();
                field.setDataType(datatype);
            } else {
                field.setDataType("http://www.w3.org/2001/XMLSchema#string");
            }
            // extra
            if (fieldConfig.has("extra")) {
                field.setExtra(fieldConfig.get("extra"));
            }
            // valueView (using current view for now)
            if (fieldConfig.has("valueView")) {
                String valueViewId = fieldConfig.get("valueView").getTextValue();
                field.setValueViewId(valueViewId);
            } 
            // editView (using current view for now)
            if (fieldConfig.has("editView")) {
                String editViewId = fieldConfig.get("editView").getTextValue();
                field.setEditViewId(editViewId);
            } 
            // createView (using current view for now)
            if (fieldConfig.has("createView")) {
                String createViewId = fieldConfig.get("createView").getTextValue();
                field.setCreateViewId(createViewId);
            } 

            // minCardinality
            if (fieldConfig.has("minCardinality")) {
                field.setMinCardinality(fieldConfig.get("minCardinality").getIntValue());
            }
            // maxCardinality
            if (fieldConfig.has("maxCardinality")) {
                field.setMaxCardinality(fieldConfig.get("maxCardinality").getIntValue());
            }
            // validationType
            if (fieldConfig.has("validationType")) {
                String validationType = fieldConfig.get("validationType").getTextValue();
                field.setValidationType(validationType);
            }           
            // isInline
            if (fieldConfig.has("inline")) {
                field.setInline(fieldConfig.get("inline").getBooleanValue());
            }
            // isEmbedded
            if (fieldConfig.has("embedded")) {
                field.setEmbedded(fieldConfig.get("embedded").getBooleanValue());
            }
            // isHidden
            if (fieldConfig.has("hidden")) {
                field.setHidden(fieldConfig.get("hidden").getBooleanValue());
            }
            // isTraversable
            if (fieldConfig.has("traversable")) {
                field.setTraversable(fieldConfig.get("traversable").getBooleanValue());
            }
            // isReadOnly            
            if (fieldConfig.has("readOnly")) {
                field.setReadOnly(fieldConfig.get("readOnly").getBooleanValue());
            } else {
                field.setReadOnly(readOnlyDefault);
            }
            // isSorted
            if (fieldConfig.has("sorted")) {
                field.setSorted(fieldConfig.get("sorted").getBooleanValue());
            }
            // isSortOrderAscending
            if (fieldConfig.has("sortedAscending")) {
                field.setSortAscending(fieldConfig.get("sortedAscending").getBooleanValue());
            }
            // isPageable
            if (fieldConfig.has("pageable")) {
                field.setPageable(fieldConfig.get("pageable").getBooleanValue());
            }
            // limit
            if (fieldConfig.has("limit")) {
                field.setLimit(fieldConfig.get("limit").getIntValue());
            }
            // isCascadingDelete
            if (fieldConfig.has("cascadingDelete")) {
                field.setCascadingDelete(fieldConfig.get("cascadingDelete").getBooleanValue());
            }
            // inverseFieldId
            if (fieldConfig.has("inverseFieldId")) {
                field.setInverseFieldId(fieldConfig.get("inverseFieldId").getTextValue());
            }
            // isEdittable
            if (fieldConfig.has("editable")) {
                field.setEditable(fieldConfig.get("editable").getBooleanValue());
            }
            // isCreatable
            if (fieldConfig.has("creatable")) {
                field.setCreatable(fieldConfig.get("creatable").getBooleanValue());
            }
            // isAddable
            if (fieldConfig.has("addable")) {
                field.setAddable(fieldConfig.get("addable").getBooleanValue());
            }
            // isRemovable
            if (fieldConfig.has("removable")) {
                field.setRemovable(fieldConfig.get("removable").getBooleanValue());
            }
            // interfaceControl
            if (fieldConfig.has("interfaceControl")) {
                String interfaceControl = fieldConfig.get("interfaceControl").getTextValue();
                field.setInterfaceControl(interfaceControl);
            }

            if (field.isReferenceField()) {
                
                // availableFieldValueTypes
                Set<PrestoType> valueTypes = new HashSet<PrestoType>();
                if (fieldConfig.has("valueTypes")) {
                    ArrayNode valueTypesArray = (ArrayNode)fieldConfig.get("valueTypes");
                    for (JsonNode valueTypeIdNode : valueTypesArray) {
                        String valueTypeId = valueTypeIdNode.getTextValue();
                        verifyDeclaredType(valueTypeId, typesMap, "valueTypes", type, field);
                        PojoType valueType = getPojoType(valueTypeId, types, schemaProvider);
                        valueTypes.add(valueType);
                    }
                }
                field.setAvailableFieldValueType(valueTypes);

                // availableFieldCreateTypes
                Set<PrestoType> createTypes = new HashSet<PrestoType>();
                if (fieldConfig.has("createTypes")) {
                    ArrayNode createTypesArray = (ArrayNode)fieldConfig.get("createTypes");
                    for (JsonNode createTypeIdNode : createTypesArray) {
                        String createTypeId = createTypeIdNode.getTextValue();
                        verifyDeclaredType(createTypeId, typesMap, "createTypes", type, field);
                        PojoType createType = getPojoType(createTypeId, types, schemaProvider);
                        createTypes.add(createType);
                    }
                }
                field.setAvailableFieldCreateType(createTypes);
            }
            return field;
        }

        private Map<String, ObjectNode> createTypesMap(ObjectNode json) {
            Map<String,ObjectNode> typesMap = new HashMap<String,ObjectNode>();
            if (json.has("types")) {
                ObjectNode typesNode = (ObjectNode)json.get("types");
                Iterator<String> typeNames = typesNode.getFieldNames();
                while (typeNames.hasNext()) {
                    String typeName = typeNames.next();
                    ObjectNode typeConfig = (ObjectNode)typesNode.get(typeName);
                    typesMap.put(typeName, typeConfig);
                }
            }
            return typesMap;
        }

        private Map<String, ObjectNode> createFieldsMap(ObjectNode json) {
            Map<String,ObjectNode> fieldsMap = new HashMap<String,ObjectNode>();
            if (json.has("fields")) {
                ObjectNode fieldsNode = (ObjectNode)json.get("fields");
                Iterator<String> fieldNames = fieldsNode.getFieldNames();
                while (fieldNames.hasNext()) {
                    String fieldName = fieldNames.next();
                    ObjectNode fieldConfig = (ObjectNode)fieldsNode.get(fieldName);
                    fieldsMap.put(fieldName, fieldConfig);
                }
            }
            return fieldsMap;
        }

        private Map<String, ObjectNode> createViewsMap(ObjectNode json) {
            Map<String,ObjectNode> viewsMap = new HashMap<String,ObjectNode>();
            if (json.has("views")) {
                ObjectNode viewsNode = (ObjectNode)json.get("views");
                Iterator<String> fieldNames = viewsNode.getFieldNames();
                while (fieldNames.hasNext()) {
                    String fieldName = fieldNames.next();
                    ObjectNode viewConfig = (ObjectNode)viewsNode.get(fieldName);
                    viewsMap.put(fieldName, viewConfig);
                }
            }
            return viewsMap;
        }

        private void verifyDeclaredType(String typeId, Map<String, ObjectNode> typesMap, String jsonField, PojoType type) {
            if (!typesMap.containsKey(typeId)) {
                throw new RuntimeException("Unknown type '" + typeId + "' in " + jsonField + " on type '" + type.getId() + "'");
            }
        }

        private void verifyDeclaredType(String typeId, Map<String, ObjectNode> typesMap, String jsonField, PojoType type, PojoField field) {
            if (!typesMap.containsKey(typeId)) {
                throw new RuntimeException("Unknown type '" + typeId + "' in " + jsonField + " in field '" + field.getId() + "' on type '" + type.getId() + "'");
            }
        }

        private  PojoType getPojoType(String typeId, Map<String,PojoType> types, PojoSchemaProvider schemaProvider) {
            PojoType type = types.get(typeId);
            if (type == null) {
                type = new PojoType(typeId, schemaProvider);
                types.put(typeId, type);
            }
            return type;
        }
        
        public PojoSchemaProvider getSchemaProvider() {
            return schemaProvider;
        }
        
    }

}
