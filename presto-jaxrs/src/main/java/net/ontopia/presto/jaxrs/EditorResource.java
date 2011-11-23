package net.ontopia.presto.jaxrs;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import net.ontopia.presto.jaxb.AvailableDatabases;
import net.ontopia.presto.jaxb.AvailableFieldTypes;
import net.ontopia.presto.jaxb.AvailableFieldValues;
import net.ontopia.presto.jaxb.AvailableTopicTypes;
import net.ontopia.presto.jaxb.Database;
import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.jaxb.RootInfo;
import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxb.TopicType;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoSession;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

@Path("/editor")
public abstract class EditorResource {

    public final static String APPLICATION_JSON_UTF8 = "application/json;charset=UTF-8";

    private @Context HttpServletRequest request;
    private @Context UriInfo uriInfo;
    
    @GET
    @Produces(RootInfo.MEDIA_TYPE)
    public Response getRootInfo() throws Exception {

        RootInfo result = new RootInfo();

        result.setVersion(0);
        result.setName("Presto - Editor REST API");

        List<Link> links = new ArrayList<Link>();
        links.add(new Link("available-databases", uriInfo.getBaseUri() + "editor/available-databases"));
        result.setLinks(links);      

        return Response.ok(result).build();
    }

    @GET
    @Produces(AvailableDatabases.MEDIA_TYPE)
    @Path("available-databases")
    public Response getDatabases() throws Exception {

        AvailableDatabases result = new AvailableDatabases();

        result.setName("Presto - Editor REST API");

        Collection<Database> databases = new ArrayList<Database>();
        for (String databaseId : getDatabaseIds()) {
            Database database = new Database();
            database.setId(databaseId);
            database.setName(getDatabaseName(databaseId));

            List<Link> links = new ArrayList<Link>();
            links.add(new Link("edit", uriInfo.getBaseUri() + "editor/database-info/" + database.getId()));
            database.setLinks(links);    

            databases.add(database);
        }
        result.setDatabases(databases);      

        return Response.ok(result).build();
    }

    @GET
    @Produces(Database.MEDIA_TYPE)
    @Path("database-info/{databaseId}")
    public Response getDatabaseInfo(
            @PathParam("databaseId") final String databaseId) throws Exception {

        PrestoSession session = createSession(databaseId);
        try {
            Database result = new Database();

            result.setId(session.getDatabaseId());
            result.setName(session.getDatabaseName());

            List<Link> links = new ArrayList<Link>();
            links.add(new Link("available-types-tree", uriInfo.getBaseUri() + "editor/available-types-tree/" + session.getDatabaseId()));
            links.add(new Link("edit-topic-by-id", uriInfo.getBaseUri() + "editor/topic/" + session.getDatabaseId() + "/{topicId}"));
            result.setLinks(links);      

            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @GET
    @Produces(Topic.MEDIA_TYPE)
    @Path("create-instance/{databaseId}/{typeId}")
    public Response createInstance(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("typeId") final String typeId) throws Exception {

        PrestoSession session = createSession(databaseId);
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();

        try {

            PrestoType type = schemaProvider.getTypeById(typeId);
            if (type == null) {
                return Response.status(Status.NOT_FOUND).build();
            }
            PrestoView view = type.getDefaultView();

            Topic result = postProcess(createPresto(session, uriInfo).getNewTopicInfo(type, view));
            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @GET
    @Produces(FieldData.MEDIA_TYPE)
    @Path("paging-field/{databaseId}/{topicId}/{viewId}/{fieldId}/{start}/{limit}")
    public Response getFieldPaging(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, 
            @PathParam("start") final int start, 
            @PathParam("limit") final int limit) throws Exception {

        PrestoSession session = createSession(databaseId);
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
        PrestoDataProvider dataProvider = session.getDataProvider();

        try {

            PrestoTopic topic = dataProvider.getTopicById(topicId);
            if (topic == null) {
                return Response.status(Status.NOT_FOUND).build();
            }

            PrestoType type = schemaProvider.getTypeById(topic.getTypeId());
            PrestoView view = type.getViewById(viewId);

            PrestoFieldUsage field = type.getFieldById(fieldId, view);
            if (field == null) {
                return Response.status(Status.NOT_FOUND).build();
            }
            
            boolean readOnlyMode = false;
            FieldData result = createPresto(session, uriInfo).getFieldInfo(topic, field, readOnlyMode, start, limit);

            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        } 
    }

    @GET
    @Produces(Topic.MEDIA_TYPE)
    @Path("create-field-instance/{databaseId}/{parentTopicId}/{parentFieldId}/{playerTypeId}")
    public Response createFieldInstance(
            @PathParam("databaseId") final String databaseId,
            @PathParam("parentTopicId") final String parentTopicId,
            @PathParam("parentFieldId") final String parentFieldId, 
            @PathParam("playerTypeId") final String playerTypeId) throws Exception {

        PrestoSession session = createSession(databaseId);
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();

        try {

            PrestoType type = schemaProvider.getTypeById(playerTypeId);
            if (type == null) {
                return Response.status(Status.NOT_FOUND).build();
            }
            PrestoView view = type.getDefaultView();

            Topic result = postProcess(createPresto(session, uriInfo).getNewTopicInfo(type, view, parentTopicId, parentFieldId));
            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("topic-data/{databaseId}/{topicId}")
    public Response getTopicData(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId) throws Exception {

        PrestoSession session = createSession(databaseId);
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
        PrestoDataProvider dataProvider = session.getDataProvider();

        try {

            PrestoTopic topic = dataProvider.getTopicById(topicId);
            if (topic == null) {
                return Response.status(Status.NOT_FOUND).build();
            }
            PrestoType type = schemaProvider.getTypeById(topic.getTypeId());

            Map<String,Object> result = createPresto(session, uriInfo).getTopicData(topic, type);
            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @DELETE
    @Produces(APPLICATION_JSON_UTF8)
    @Path("topic/{databaseId}/{topicId}")
    public Response deleteTopic(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId) throws Exception {

        PrestoSession session = createSession(databaseId);
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
        PrestoDataProvider dataProvider = session.getDataProvider();

        try {

            PrestoTopic topic = dataProvider.getTopicById(topicId);

            if (topic == null) {
                // 200
                ResponseBuilder builder = Response.ok();
                return builder.build();        
            } else {
                PrestoType type = schemaProvider.getTypeById(topic.getTypeId());
                if (type.isRemovable() && createPresto(session, uriInfo).deleteTopic(topic, type)) {          
                    // 200
                    ResponseBuilder builder = Response.ok();
                    return builder.build();
                } else {
                    // 403
                    ResponseBuilder builder = Response.status(Status.FORBIDDEN);
                    return builder.build();
                }
            }

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @GET
    @Produces(Topic.MEDIA_TYPE)
    @Path("topic/{databaseId}/{topicId}")
    public Response getTopicInDefaultView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId,
            @QueryParam("readOnly") final boolean readOnly) throws Exception {

        PrestoSession session = createSession(databaseId);
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
        PrestoDataProvider dataProvider = session.getDataProvider();

        try {

            PrestoTopic topic = dataProvider.getTopicById(topicId);
            if (topic == null) {
                return Response.status(Status.NOT_FOUND).build();
            }
            PrestoType type = schemaProvider.getTypeById(topic.getTypeId());
            PrestoView view = type.getDefaultView();

            Topic result = postProcess(createPresto(session, uriInfo).getTopicInfo(topic, type, view, readOnly));
            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @GET
    @Produces(Topic.MEDIA_TYPE)
    @Path("topic/{databaseId}/{topicId}/{viewId}")
    public Response getTopicInView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId,
            @PathParam("viewId") final String viewId,
            @QueryParam("readOnly") final boolean readOnly) throws Exception {

        PrestoSession session = createSession(databaseId);
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
        PrestoDataProvider dataProvider = session.getDataProvider();

        try {

            PrestoTopic topic = dataProvider.getTopicById(topicId);
            if (topic == null) {
                return Response.status(Status.NOT_FOUND).build();
            }
            PrestoType type = schemaProvider.getTypeById(topic.getTypeId());
            PrestoView view = type.getViewById(viewId);

            Topic result = postProcess(createPresto(session, uriInfo).getTopicInfo(topic, type, view, readOnly));
            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @PUT
    @Produces(Topic.MEDIA_TYPE)
    @Consumes(Topic.MEDIA_TYPE)
    @Path("topic/{databaseId}/{topicId}/{viewId}")
    public Response updateTopic(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId, Topic topicData) throws Exception {

        PrestoSession session = createSession(databaseId);
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
        PrestoDataProvider dataProvider = session.getDataProvider();

        try {

            PrestoTopic topic = null;
            PrestoType type;
            if (topicId.startsWith("_")) {
                type = schemaProvider.getTypeById(topicId.substring(1));
            } else {
                topic = dataProvider.getTopicById(topicId);
                if (topic == null) {
                    return Response.status(Status.NOT_FOUND).build();
                }
                type = schemaProvider.getTypeById(topic.getTypeId());
            }

            PrestoView view = type.getViewById(viewId);

            topic = createPresto(session, uriInfo).updateTopic(topic, type, view, preProcess(topicData));

            Topic result = createPresto(session, uriInfo).getTopicInfo(topic, type, view, false);
            String id = result.getId();
            session.commit();
            onTopicUpdated(id);

            result = postProcess(result);
            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();
        }
    }

    @POST
    @Produces(FieldData.MEDIA_TYPE)
    @Consumes(FieldData.MEDIA_TYPE)
    @Path("add-field-values-at-index/{databaseId}/{topicId}/{viewId}/{fieldId}/{index}")
    public Response addFieldValuesAtIndex( 
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, 
            @PathParam("index") final Integer index, FieldData fieldData) throws Exception {

        PrestoSession session = createSession(databaseId);
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
        PrestoDataProvider dataProvider = session.getDataProvider();

        try {

            PrestoTopic topic = dataProvider.getTopicById(topicId);
            if (topic == null) {
                return Response.status(Status.NOT_FOUND).build();
            }

            PrestoType type = schemaProvider.getTypeById(topic.getTypeId());
            PrestoView view = type.getViewById(viewId);

            PrestoFieldUsage field = type.getFieldById(fieldId, view);

            if (field.isAddable() || field.isCreatable()) {
                FieldData result = createPresto(session, uriInfo).addFieldValues(topic, type, field, index, fieldData);
    
                String id = topic.getId();
    
                session.commit();
                onTopicUpdated(id);
    
                return Response.ok(result).build();
            
            } else {
                // 403
                ResponseBuilder builder = Response.status(Status.FORBIDDEN);
                return builder.build();
            }

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        } 
    }

    @POST
    @Produces(FieldData.MEDIA_TYPE)
    @Consumes(FieldData.MEDIA_TYPE)
    @Path("move-field-values-to-index/{databaseId}/{topicId}/{viewId}/{fieldId}/{index}")
    public Response moveFieldValuesToIndex( 
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, 
            @PathParam("index") final Integer index, FieldData fieldData) throws Exception {

        return addFieldValuesAtIndex(databaseId, topicId, viewId, fieldId, index, fieldData);
    }

    @POST
    @Produces(FieldData.MEDIA_TYPE)
    @Consumes(FieldData.MEDIA_TYPE)
    @Path("add-field-values/{databaseId}/{topicId}/{viewId}/{fieldId}")
    public Response addFieldValues(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, FieldData fieldData) throws Exception {

        Integer index = null;
        return addFieldValuesAtIndex(databaseId, topicId, viewId, fieldId, index, fieldData);
    }

    @POST
    @Produces(FieldData.MEDIA_TYPE)
    @Consumes(FieldData.MEDIA_TYPE)
    @Path("remove-field-values/{databaseId}/{topicId}/{viewId}/{fieldId}")
    public Response removeFieldValues(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, FieldData fieldData) throws Exception {

        PrestoSession session = createSession(databaseId);
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
        PrestoDataProvider dataProvider = session.getDataProvider();

        try {

            PrestoTopic topic = dataProvider.getTopicById(topicId);
            if (topic == null) {
                return Response.status(Status.NOT_FOUND).build();
            }

            PrestoType type = schemaProvider.getTypeById(topic.getTypeId());
            PrestoView view = type.getViewById(viewId);

            PrestoFieldUsage field = type.getFieldById(fieldId, view);

            if (field.isRemovable()) {

                FieldData result =  createPresto(session, uriInfo).removeFieldValues(topic, type, field, fieldData);
    
                String id = topic.getId();    
                session.commit();
                onTopicUpdated(id);
    
                return Response.ok(result).build();
                
            } else {
                // 403
                ResponseBuilder builder = Response.status(Status.FORBIDDEN);
                return builder.build();
            }
        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        } 
    }

    @GET
    @Produces(AvailableFieldValues.MEDIA_TYPE)
    @Path("available-field-values/{databaseId}/{topicId}/{viewId}/{fieldId}")
    public Response getAvailableFieldValues( 
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId) throws Exception {

        PrestoSession session = createSession(databaseId);
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
        PrestoDataProvider dataProvider = session.getDataProvider();

        try {

            PrestoTopic topic = null;
            PrestoType type;
            if (topicId.startsWith("_")) {
                type = schemaProvider.getTypeById(topicId.substring(1));
            } else {
                topic = dataProvider.getTopicById(topicId);
                if (topic == null) {
                    return Response.status(Status.NOT_FOUND).build();
                }
                type = schemaProvider.getTypeById(topic.getTypeId());
            }

            PrestoView view = type.getViewById(viewId);

            PrestoFieldUsage field = type.getFieldById(fieldId, view);

            Collection<PrestoTopic> availableFieldValues = dataProvider.getAvailableFieldValues(field);

            AvailableFieldValues result = createPresto(session, uriInfo).createFieldInfoAllowed(field, availableFieldValues);
            return Response.ok(result).build();
            
        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @GET
    @Produces(AvailableFieldTypes.MEDIA_TYPE)
    @Path("available-field-types/{databaseId}/{topicId}/{viewId}/{fieldId}")
    public Response getAvailableFieldTypes( 
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId) throws Exception {

        PrestoSession session = createSession(databaseId);
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
        PrestoDataProvider dataProvider = session.getDataProvider();

        try {

            PrestoTopic topic = null;
            PrestoType type;
            if (topicId.startsWith("_")) {
                type = schemaProvider.getTypeById(topicId.substring(1));
            } else {
                topic = dataProvider.getTopicById(topicId);
                if (topic == null) {
                    return Response.status(Status.NOT_FOUND).build();
                }
                type = schemaProvider.getTypeById(topic.getTypeId());
            }

            PrestoView view = type.getViewById(viewId);

            PrestoFieldUsage field = type.getFieldById(fieldId, view);

            AvailableFieldTypes result = new AvailableFieldTypes();
            result.setId(field.getId());
            result.setName(field.getName());

            if (field.isCreatable()) {
                Collection<PrestoType> availableFieldCreateTypes = field.getAvailableFieldCreateTypes();
                List<TopicType> types = new ArrayList<TopicType>(availableFieldCreateTypes.size());
                for (PrestoType createType : availableFieldCreateTypes) {
                    types.add(createPresto(session, uriInfo).getCreateFieldInstance(topic, type, field, createType));
                }                
                result.setTypes(types);
            } else {
                result.setTypes(new ArrayList<TopicType>());
            }

            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @GET
    @Produces(AvailableTopicTypes.MEDIA_TYPE)
    @Path("available-types-tree/{databaseId}")
    public Response getAvailableTypesTree(@PathParam("databaseId") final String databaseId) throws Exception {

        PrestoSession session = createSession(databaseId);
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();

        try {

            AvailableTopicTypes result = new AvailableTopicTypes();
            result.setTypes(createPresto(session, uriInfo).getAvailableTypes(schemaProvider.getRootTypes(), true));      

            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    // overridable methods

    protected abstract PrestoSession createSession(String databaseId);

    protected Presto createPresto(PrestoSession session, UriInfo uriInfo) {
        return new Presto(session, uriInfo) {
            @Override
            public FieldData getFieldInfo(PrestoTopic topic, PrestoFieldUsage field, boolean readOnlyMode, int offset, int limit) {
                FieldData fieldData = super.getFieldInfo(topic, field, readOnlyMode, offset, limit);
                
                List<FieldData.Message> messages = new ArrayList<FieldData.Message>();
                // Move messages from extra.messages to fieldData.messages
                Object extra = fieldData.getExtra();
                if (extra != null && extra instanceof ObjectNode) {
                    ObjectNode extraNode = (ObjectNode)extra;
                    if (extraNode.get("messages") != null) {
                        for (JsonNode messageNode : extraNode.get("messages")) {
                            String type = messageNode.get("type").getTextValue();
                            String message = messageNode.get("message").getTextValue();
                            messages.add(new FieldData.Message(type, message));
                        }
                        extraNode.remove("messages");
                    }
                }
                
                if (fieldData.getMessages() != null) {
                    fieldData.getMessages().addAll(messages);
                } else {
                    fieldData.setMessages(messages);
                }
                return fieldData;
            }
            @Override
            protected Collection<String> getVariableValues(String variable) {
                return EditorResource.this.getVariableValues(variable);
            }
        };
    }
    
    protected Collection<String> getVariableValues(String variable) {
        if (variable.equals("now")) {
            return Collections.singletonList(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date()));
        } else if (variable.equals("username")) {
            return Collections.singletonList(request.getRemoteUser());
        }
        return Collections.emptyList();
    }
    
    protected abstract Collection<String> getDatabaseIds();

    protected abstract String getDatabaseName(String databaseId);

    protected void onTopicUpdated(String topicId) {      
    }

    protected Topic preProcess(Topic topicInfo) {
        return topicInfo;
    }

    protected Topic postProcess(Topic topicInfo) {
        return topicInfo;
    }

}
