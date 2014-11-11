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
import net.ontopia.presto.spi.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
            ObjectNode objectNode = Utils.DEFAULT_OBJECT_MAPPER.readValue(reader, ObjectNode.class);
            
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
                defaultsReadOnly = defaultsReadOnlyNode.booleanValue();
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
            String name = typeConfig.get("name").textValue();
            type.setName(name);

            // readOnly
            if (typeConfig.has("readOnly")) {
                readOnlyDefault = typeConfig.get("readOnly").booleanValue();
            }
            
            // inline
            if (typeConfig.has("inline")) {
                type.setInline(typeConfig.get("inline").booleanValue());
            }
            // hidden
            if (typeConfig.has("hidden")) {
                type.setHidden(typeConfig.get("hidden").booleanValue());
            }
            // lazy
            if (typeConfig.has("lazy")) {
                type.setLazy(typeConfig.get("lazy").booleanValue());
            }
            // creatable
            if (typeConfig.has("creatable")) {
                type.setCreatable(typeConfig.get("creatable").booleanValue());
            }
            // removable
            if (typeConfig.has("removable")) {
                type.setRemovable(typeConfig.get("removable").booleanValue());
            }
            // removableCascadingDelete
            if (typeConfig.has("removableCascadingDelete")) {
                type.setRemovableCascadingDelete(typeConfig.get("removableCascadingDelete").booleanValue());
            }

            // extends
            if (typeConfig.has("extends")) {
                String superTypeId = typeConfig.get("extends").textValue();
                verifyDeclaredType(superTypeId, typesMap, "extends", type);
                PojoType superType = getPojoType(superTypeId, types, schemaProvider);
                //                type.setSuperType(superType);
                superType.addDirectSubType(type);
            }
            // defaultView
            String defaultViewId = null;
            if (typeConfig.has("defaultView")) {
                defaultViewId = typeConfig.get("defaultView").textValue();
                type.setDefaultViewId(defaultViewId);
            } 
            // createView
            if (typeConfig.has("createView")) {
                String createViewId = typeConfig.get("createView").textValue();
                type.setCreateViewId(createViewId);
            } else if (defaultViewId != null){
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
                    viewId = viewNode.textValue();                        
                    viewConfig = viewsMap.get(viewId);
                } else if (viewNode.isObject()) {
                    viewConfig = (ObjectNode)viewNode;
                    viewId = viewConfig.get("id").textValue();
                } else {
                    throw new RuntimeException("Invalid view declaration or view reference: " + viewNode);
                }
               
                PojoView view = new PojoView(viewId);
                type.addView(view);

                // name
                String viewName = viewConfig.get("name").textValue();
                view.setName(viewName);

                // readOnly
                if (viewConfig.has("readOnly")) {
                    readOnlyDefault = viewConfig.get("readOnly").booleanValue();
                }

                // type
                ViewType viewType = ViewType.NORMAL_VIEW;
                if (viewConfig.has("type")) {
                    viewType = ViewType.findByTypeId(viewConfig.get("type").textValue());
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
            if (fieldsArray != null) {
                for (JsonNode fieldNode : fieldsArray) {
                    PojoField field = createField(type, view, readOnlyDefault, fieldNode);
                    type.addField(field);
                }
            }
        }

        private PojoField createField(PojoType type, PojoView view, boolean readOnlyDefault, JsonNode fieldNode) {
            String fieldId;
            ObjectNode fieldConfig;
            if (fieldNode.isTextual()) {
                fieldId = fieldNode.textValue();                        
                fieldConfig = fieldsMap.get(fieldId);
            } else if (fieldNode.isObject()) {
                fieldConfig = (ObjectNode)fieldNode;
                fieldId = fieldConfig.get("id").textValue();
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
            PojoField field = new PojoField(fieldId, type, view, schemaProvider);
            
            // actualId
            if (fieldConfig.has("actualId")) {
                field.setActualId(fieldConfig.get("actualId").textValue());
            }

            // name
            if (fieldConfig.has("name")) {
                String fieldName = fieldConfig.get("name").textValue();                        
                field.setName(fieldName);
            } else {
                field.setName(fieldId);
            }
            // isNameField
            if (fieldConfig.has("nameField")) {
                field.setNameField(fieldConfig.get("nameField").booleanValue());
            }

            // isPrimitiveField/isReferenceField

            // dataType
            if (fieldConfig.has("datatype")) {
                String datatype = fieldConfig.get("datatype").textValue();
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
                String valueViewId = fieldConfig.get("valueView").textValue();
                field.setValueViewId(valueViewId);
            } 
            // editView (using current view for now)
            if (fieldConfig.has("editView")) {
                String editViewId = fieldConfig.get("editView").textValue();
                field.setEditViewId(editViewId);
            } 
            // createView (using current view for now)
            if (fieldConfig.has("createView")) {
                String createViewId = fieldConfig.get("createView").textValue();
                field.setCreateViewId(createViewId);
            } 

            // minCardinality
            if (fieldConfig.has("minCardinality")) {
                field.setMinCardinality(fieldConfig.get("minCardinality").intValue());
            }
            // maxCardinality
            if (fieldConfig.has("maxCardinality")) {
                field.setMaxCardinality(fieldConfig.get("maxCardinality").intValue());
            }
            // validationType
            if (fieldConfig.has("validationType")) {
                String validationType = fieldConfig.get("validationType").textValue();
                field.setValidationType(validationType);
            }           
            // isInline
            if (fieldConfig.has("inline")) {
                field.setInline(fieldConfig.get("inline").booleanValue());
                if (fieldConfig.has("inlineReference")) {
                    field.setInlineReference(fieldConfig.get("inlineReference").textValue());
                }
            }
            // isParentRelation
            if (fieldConfig.has("parentRelation")) {
                field.setParentRelation(fieldConfig.get("parentRelation").booleanValue());
            }
            // isEmbedded
            if (fieldConfig.has("embedded")) {
                field.setEmbedded(fieldConfig.get("embedded").booleanValue());
            }
            // isHidden
            if (fieldConfig.has("hidden")) {
                field.setHidden(fieldConfig.get("hidden").booleanValue());
            }
            // isTraversable
            if (fieldConfig.has("traversable")) {
                field.setTraversable(fieldConfig.get("traversable").booleanValue());
            }
            // isReadOnly            
            if (fieldConfig.has("readOnly")) {
                field.setReadOnly(fieldConfig.get("readOnly").booleanValue());
            } else {
                field.setReadOnly(readOnlyDefault);
            }
            // isSorted
            if (fieldConfig.has("sorted")) {
                field.setSorted(fieldConfig.get("sorted").booleanValue());
            }
            // isSortOrderAscending
            if (fieldConfig.has("sortedAscending")) {
                field.setSortAscending(fieldConfig.get("sortedAscending").booleanValue());
            }
            // isPageable
            if (fieldConfig.has("pageable")) {
                field.setPageable(fieldConfig.get("pageable").booleanValue());
            }
            // limit
            if (fieldConfig.has("limit")) {
                field.setLimit(fieldConfig.get("limit").intValue());
            }
            // isCascadingDelete
            if (fieldConfig.has("cascadingDelete")) {
                field.setCascadingDelete(fieldConfig.get("cascadingDelete").booleanValue());
            }
            // inverseFieldId
            if (fieldConfig.has("inverseFieldId")) {
                field.setInverseFieldId(fieldConfig.get("inverseFieldId").textValue());
            }
            // isEdittable
            if (fieldConfig.has("editable")) {
                field.setEditable(fieldConfig.get("editable").booleanValue());
            }
            // isCreatable
            if (fieldConfig.has("creatable")) {
                field.setCreatable(fieldConfig.get("creatable").booleanValue());
            }
            // isAddable
            if (fieldConfig.has("addable")) {
                field.setAddable(fieldConfig.get("addable").booleanValue());
            }
            // isRemovable
            if (fieldConfig.has("removable")) {
                field.setRemovable(fieldConfig.get("removable").booleanValue());
            }
            // interfaceControl
            if (fieldConfig.has("interfaceControl")) {
                String interfaceControl = fieldConfig.get("interfaceControl").textValue();
                field.setInterfaceControl(interfaceControl);
            }

            if (field.isReferenceField()) {
                
                // availableFieldValueTypes
                Set<PrestoType> valueTypes = new HashSet<PrestoType>();
                if (fieldConfig.has("valueTypes")) {
                    ArrayNode valueTypesArray = (ArrayNode)fieldConfig.get("valueTypes");
                    for (JsonNode valueTypeIdNode : valueTypesArray) {
                        String valueTypeId = valueTypeIdNode.textValue();
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
                        String createTypeId = createTypeIdNode.textValue();
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
                Iterator<String> typeNames = typesNode.fieldNames();
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
                Iterator<String> fieldNames = fieldsNode.fieldNames();
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
                Iterator<String> fieldNames = viewsNode.fieldNames();
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
