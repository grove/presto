package net.ontopia.presto.jaxrs;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

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
import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxrs.Presto.PrestoContextField;
import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoChanges;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoDataProvider.ChangeSetHandler;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;
import net.ontopia.presto.spi.PrestoView;

@Path("/editor")
public abstract class EditorResource {

    public final static String APPLICATION_JSON_UTF8 = "application/json;charset=UTF-8";

    @Context HttpServletRequest request;
    private @Context UriInfo uriInfo;
    
    @GET
    @Produces(APPLICATION_JSON_UTF8)
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
    @Produces(APPLICATION_JSON_UTF8)
    @Path("available-databases")
    public Response getDatabases() throws Exception {

        AvailableDatabases result = getAvailableDatabasesInfo();
        return Response.ok(result).build();
    }

    protected AvailableDatabases getAvailableDatabasesInfo() {
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
        return result;
    }
    
    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("database-info/{databaseId}")
    public Response getDatabaseInfo(
            @PathParam("databaseId") final String databaseId) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            Database result = session.getDatabaseInfo();
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
    @Path("create-instance/{databaseId}/{typeId}")
    public Response createInstance(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("typeId") final String typeId) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            PrestoSchemaProvider schemaProvider = session.getSchemaProvider();

            PrestoType type = schemaProvider.getTypeById(typeId);
            if (type == null) {
                return Response.status(Status.NOT_FOUND).build();
            }

            boolean readOnly = false;
            PrestoContext context = PrestoContext.create(type, type.getCreateView(), readOnly);

            TopicView result = session.getNewTopicView(context);
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
    @Path("create-field-instance/{databaseId}/{path}/{typeId}")
    public Response createFieldInstance(
            @PathParam("databaseId") final String databaseId,
            @PathParam("path") final String path,
            @PathParam("typeId") final String typeId) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            PrestoSchemaProvider schemaProvider = session.getSchemaProvider();

            PrestoType type = schemaProvider.getTypeById(typeId);
            if (type == null) {
                return Response.status(Status.NOT_FOUND).build();
            }

            boolean readOnly = false;
            
            PrestoContextField contextField = session.getContextField(path, readOnly);

            TopicView result = session.getNewTopicView(contextField.getContext(), contextField.getField(), type);
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
    @Path("paging-field/{databaseId}/{topicId}/{viewId}/{fieldId}/{start}/{limit}")
    public Response getFieldPaging(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, 
            @PathParam("start") final int start, 
            @PathParam("limit") final int limit) throws Exception {
        
        Presto session = createPresto(databaseId);

        try {
            boolean readOnly = false;

            PrestoContext context = PrestoContext.create(session, Links.deskull(topicId), readOnly);

            if (context.isMissingTopic()) {
                return Response.status(Status.NOT_FOUND).build();
            }

            PrestoFieldUsage field = context.getFieldById(fieldId);
            if (field == null) {
                return Response.status(Status.NOT_FOUND).build();
            }
            
            FieldData result = session.getFieldData(context, field, start, limit, true);
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
    @Path("topic-view/{databaseId}/{topicId}/{viewId}")
    public Response deleteTopicView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId,
            @PathParam("viewId") final String viewId) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
            PrestoDataProvider dataProvider = session.getDataProvider();

            PrestoTopic topic = dataProvider.getTopicById(Links.deskull(topicId));

            if (topic == null) {
                // 404
                return Response.status(Status.NOT_FOUND).build();        
            } else {
                PrestoType type = schemaProvider.getTypeById(topic.getTypeId());
                if (type.isRemovable()) {
                    session.deleteTopic(topic, type);          
                    // 204
                    return Response.noContent().build();
                } else {
                    // 403
                    return Response.status(Status.FORBIDDEN).build();
                }
            }

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @DELETE
    @Produces(APPLICATION_JSON_UTF8)
    @Path("topic-view-inline/{databaseId}/{path}/{topicId}/{viewId}")
    public Response deleteTopicViewInline(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("path") final String path,
            @PathParam("topicId") final String topicId,
            @PathParam("viewId") final String viewId) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            boolean readOnly = false;
            PrestoContext context = session.getInlineTopic(path, topicId, viewId, readOnly);

            if (context == null || context.isMissingTopic()) {
                // 404
                return Response.status(Status.NOT_FOUND).build();        
            } else {
                PrestoType type = context.getType();
                if (type.isRemovable()) {
                    PrestoTopic topic = context.getTopic();
                    PrestoContext parentContext = context.getParentContext();
                    PrestoFieldUsage parentField = context.getParentField();
                    PrestoTopic parentTopicAfterSave = session.removeFieldValues(parentContext, parentField, Collections.singletonList(topic));

                    FieldData fieldData = session.getFieldData(parentTopicAfterSave, parentField, readOnly);
                    return Response.ok(fieldData).build();

//                    // 204
//                    return Response.noContent().build();
                } else {
                    // 403
                    return Response.status(Status.FORBIDDEN).build();
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
    @Produces(APPLICATION_JSON_UTF8)
    @Path("topic/{databaseId}/{topicId}")
    public Response getTopicInDefaultView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId,
            @QueryParam("readOnly") final boolean readOnly) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            PrestoContext context = PrestoContext.create(session, Links.deskull(topicId), readOnly);

            if (context.isMissingTopic()) {
                return Response.status(Status.NOT_FOUND).build();
            }

            Topic result = session.getTopicAndProcess(context);
            
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
    @Path("topic/{databaseId}/{topicId}/{viewId}")
    public Response getTopicInView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId,
            @PathParam("viewId") final String viewId,
            @QueryParam("readOnly") final boolean readOnly) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            PrestoContext context = PrestoContext.create(session, Links.deskull(topicId), viewId, readOnly);

            if (context.isMissingTopic()) {
                return Response.status(Status.NOT_FOUND).build();
            }

            Topic result = session.getTopicAndProcess(context);
            
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
    @Path("topic-inline/{databaseId}/{path}/{topicId}/{viewId}")
    public Response getTopicInlineInView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("path") final String path,
            @PathParam("topicId") final String topicId,
            @PathParam("viewId") final String viewId,
            @QueryParam("readOnly") final boolean readOnly) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            PrestoContext context = session.getInlineTopic(path, topicId, viewId, readOnly);

            if (context == null || context.isMissingTopic()) {
                return Response.status(Status.NOT_FOUND).build();
            }
            
            Topic result = session.getTopicAndProcess(context);
            
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
    @Path("topic-view/{databaseId}/{topicId}")
    public Response getTopicViewInDefaultView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId,
            @QueryParam("readOnly") final boolean readOnly) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            PrestoContext context = PrestoContext.create(session, Links.deskull(topicId), readOnly);

            if (context.isMissingTopic()) {
                return Response.status(Status.NOT_FOUND).build();
            }

            TopicView result = session.getTopicViewAndProcess(context);
            
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
    @Path("topic-view/{databaseId}/{topicId}/{viewId}")
    public Response getTopicViewInView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId,
            @PathParam("viewId") final String viewId,
            @QueryParam("readOnly") final boolean readOnly) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            PrestoContext context = PrestoContext.create(session, Links.deskull(topicId), viewId, readOnly);

            if (context.isMissingTopic()) {
                return Response.status(Status.NOT_FOUND).build();
            }

            TopicView result = session.getTopicViewAndProcess(context);
            
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
    @Path("topic-view-inline/{databaseId}/{path}/{topicId}/{viewId}")
    public Response getTopicViewInlineInView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("path") final String path,
            @PathParam("topicId") final String topicId,
            @PathParam("viewId") final String viewId,
            @QueryParam("readOnly") final boolean readOnly) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            PrestoContext context = session.getInlineTopic(path, topicId, viewId, readOnly);

            if (context == null || context.isMissingTopic()) {
                return Response.status(Status.NOT_FOUND).build();
            }
            
            TopicView result = session.getTopicViewAndProcess(context);
            
            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @PUT
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("validate-topic/{databaseId}/{topicId}/{viewId}")
    public Response validateTopic(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId, TopicView topicView) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            // NOTE: the topicId is the topic that requested the validation, but the 
            // validation needs to start with the topicId of the received topicView. The 
            // former is a descendant of the latter.
            String topicViewTopicId = topicView.getTopicId();
            
            boolean readOnly = false;
            PrestoContext context = PrestoContext.create(session, topicViewTopicId, viewId, readOnly);

            if (context.isMissingTopic()) {
                return Response.status(Status.NOT_FOUND).build();
            }
            
            TopicView result = session.validateTopic(context, topicView);

            session.commit();

            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();
        }
    }

    @PUT
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("topic-view-parent/{databaseId}/{topicId}/{viewId}/{parentTopicId}/{parentViewId}/{parentFieldId}")
    public Response updateTopicViewParent(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId, 
            @PathParam("parentTopicId") final String parentTopicId, 
            @PathParam("parentViewId") final String parentViewId, 
            @PathParam("parentFieldId") final String parentFieldId, TopicView topicView) throws Exception {

        // TODO: replace this method with updateTopicViewInline :)
        
        Presto session = createPresto(databaseId);

        try {
            boolean readOnly = false;
            PrestoContext parentContext = PrestoContext.create(session, Links.deskull(parentTopicId), parentViewId, readOnly);

            PrestoDataProvider dataProvider = session.getDataProvider();
            PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
            PrestoTopic parentTopic = dataProvider.getTopicById(parentTopicId);
            String parentTypeId = parentTopic.getTypeId();
            PrestoType parentType = schemaProvider.getTypeById(parentTypeId);
            PrestoView parentView = parentType.getViewById(parentViewId);
            PrestoFieldUsage parentField = parentType.getFieldById(parentFieldId, parentView);
            
            PrestoContext context = PrestoContext.createSubContext(session, parentContext, parentField, topicId, viewId, readOnly);
//                    (session, Links.deskull(topicId), viewId, readOnly);

            if (context.isMissingTopic()) {
                return Response.status(Status.NOT_FOUND).build();
            }
                    
            TopicView result = session.updateTopic(context, topicView);
            session.commit();

            if (context.isNewTopic()) {
                // return Topic if new, otherwise TopicView
                String newTopicId = result.getTopicId();
                if (newTopicId == null) {
                    // WARN: probably means that the topic was not valid
                    return Response.ok(result).build();
                }
            }
            if (parentContext.isNewTopic()) {
                // NOTE: if parent does not exist yet then we have to return the created topic-view
                return Response.ok(result).build();
                
            } else {
                // update parent field and return it
                FieldData fieldData = session.createFieldDataForParent(parentContext, parentFieldId, readOnly, context, result);
                return addFieldValues(databaseId, parentTopicId, parentViewId, parentFieldId, fieldData);
            }            
        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();
        }
        
    }
    
    @PUT
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("topic-view/{databaseId}/{topicId}/{viewId}")
    public Response updateTopicView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId, TopicView topicView) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            boolean readOnly = false;
            PrestoContext context = PrestoContext.create(session, Links.deskull(topicId), viewId, readOnly);

            if (context.isMissingTopic()) {
                return Response.status(Status.NOT_FOUND).build();
            }

            TopicView result = session.updateTopic(context, topicView);
            session.commit();

            if (context.isNewTopic()) {
                // return Topic if new, otherwise TopicView
                String newTopicId = result.getTopicId();
                if (newTopicId == null) {
                    // WARN: probably means that the topic was not valid
                    return Response.ok(result).build();
                } else {
                    return getTopicInDefaultView(databaseId, newTopicId, readOnly);
                }
            }
            return Response.ok(result).build();
            
        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();
        }
    }
    
    @PUT
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("topic-view-inline/{databaseId}/{path}/{topicId}/{viewId}")
    public Response updateTopicViewInline(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("path") final String path, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId, TopicView topicView) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            boolean readOnly = false;
            PrestoContext context = session.getInlineTopic(path, topicId, viewId, readOnly);

            if (context == null || context.isMissingTopic()) {
                return Response.status(Status.NOT_FOUND).build();
            }

            boolean returnParent = true;
            TopicView result = session.updateTopicInline(context, topicView, returnParent);
            session.commit();

//            if (context.isNewTopic()) {
//                // return Topic if new, otherwise TopicView
//                String newTopicId = result.getTopicId();
//                if (newTopicId == null) {
//                    // WARN: probably means that the topic was not valid
//                    return Response.ok(result).build();
//                } else {
//                    return getTopicInDefaultView(databaseId, newTopicId, readOnly);
//                }
//            }
            return Response.ok(result).build();
            
        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();
        }
    }

    @POST
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("add-field-values-at-index/{databaseId}/{topicId}/{viewId}/{fieldId}/{index}")
    public Response addFieldValuesAtIndex( 
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, 
            @PathParam("index") final Integer index, FieldData fieldData) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            boolean readOnly = false;
            PrestoContext context = PrestoContext.create(session, Links.deskull(topicId), viewId, readOnly);

            if (context.isMissingTopic()) {
                return Response.status(Status.NOT_FOUND).build();
            }

            PrestoFieldUsage field = context.getFieldById(fieldId);

            if (field.isAddable() || field.isCreatable()) {
                FieldData result = session.addFieldValues(context, field, index, fieldData);
    
                session.commit();
    
                return Response.ok(result).build();
            
            } else {
                // 403
                return Response.status(Status.FORBIDDEN).build();
            }

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        } 
    }

    @POST
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
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
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
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
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("remove-field-values/{databaseId}/{topicId}/{viewId}/{fieldId}")
    public Response removeFieldValues(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, FieldData fieldData) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            boolean readOnly = false;
            PrestoContext context = PrestoContext.create(session, Links.deskull(topicId), viewId, readOnly);

            if (context.isMissingTopic()) {
                return Response.status(Status.NOT_FOUND).build();
            }

            PrestoFieldUsage field = context.getFieldById(fieldId);

            if (field.isRemovable()) {
                FieldData result =  session.removeFieldValues(context, field, fieldData);
    
                session.commit();
    
                return Response.ok(result).build();
                
            } else {
                // 403
                return Response.status(Status.FORBIDDEN).build();
            }
        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        } 
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("available-field-values/{databaseId}/{topicId}/{viewId}/{fieldId}")
    public Response getAvailableFieldValues( 
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId) throws Exception {
        return getAvailableFieldValues(databaseId, topicId, viewId, fieldId, null);
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("available-field-values-query/{databaseId}/{topicId}/{viewId}/{fieldId}")
    public Response getAvailableFieldValues( 
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId,
            @QueryParam("query") final String query) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            boolean readOnly = false;
            PrestoContext context = PrestoContext.create(session, Links.deskull(topicId), viewId, readOnly);

            if (context.isMissingTopic()) {
                return Response.status(Status.NOT_FOUND).build();
            }

            PrestoFieldUsage field = context.getFieldById(fieldId);
            
            AvailableFieldValues result = session.getAvailableFieldValuesInfo(context, field, query);
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
    @Path("available-field-types/{databaseId}/{topicId}/{viewId}/{fieldId}")
    public Response getAvailableFieldTypes( 
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            boolean readOnly = false;
            PrestoContext context = PrestoContext.create(session, Links.deskull(topicId), viewId, readOnly);

            if (context.isMissingTopic()) {
                return Response.status(Status.NOT_FOUND).build();
            }

            PrestoFieldUsage field = context.getFieldById(fieldId);

            AvailableFieldTypes result = session.getAvailableFieldTypesInfo(context, field);
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
    @Path("available-types-tree/{databaseId}")
    public Response getAvailableTypesTree(@PathParam("databaseId") final String databaseId) throws Exception {

        Presto session = createPresto(databaseId);

        try {
            AvailableTopicTypes result = session.getAvailableTypesInfo(true);
            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    // overridable methods

    public class EditorResourcePresto extends Presto {
        
        public EditorResourcePresto(String databaseId, String databaseName, PrestoSchemaProvider schemaProvider, PrestoDataProvider dataProvider) {
            super(databaseId, databaseName, schemaProvider, dataProvider);
        }
        
        @Override
        public URI getBaseUri() {
            return uriInfo.getBaseUri();
        }
        
        @Override
        protected ChangeSetHandler getChangeSetHandler() {
            return new DefaultChangeSetHandler() {

                @Override
                protected PrestoSchemaProvider getSchemaProvider() {
                    return EditorResourcePresto.this.getSchemaProvider();
                }
                
                @Override
                public void onAfterSave(PrestoChangeSet changeSet, PrestoChanges changes) {
                    for (PrestoUpdate create : changes.getCreated()) {
                        EditorResource.this.onTopicCreated(create.getTopicAfterSave());
                    }
                    for (PrestoUpdate update : changes.getUpdated()) {
                        if (update.isTopicUpdated()) {
                            EditorResource.this.onTopicUpdated(update.getTopic());
                        }
                    }
                    for (PrestoTopic delete : changes.getDeleted()) {
                        EditorResource.this.onTopicDeleted(delete);                        
                    }
                }

                @Override
                protected Collection<String> getVariableValues(PrestoTopic topic, PrestoType type, PrestoField field, String variable) {
                    if (variable.equals("now")) {
                        return Collections.singletonList(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date()));
                    } else if (variable.equals("username")) {
                        if (request != null) {
                            String remoteUser = request.getRemoteUser();
                            if (remoteUser != null) {
                                return Collections.singletonList(remoteUser);
                            }
                        }
                    }
                    return Collections.emptyList();
                }
            };
        }
    }
    
    protected abstract Presto createPresto(String databaseId);
    
    protected abstract Collection<String> getDatabaseIds();

    protected abstract String getDatabaseName(String databaseId);

    protected void onTopicCreated(PrestoTopic topic) {      
    }

    protected void onTopicUpdated(PrestoTopic topic) {      
    }
    
    protected void onTopicDeleted(PrestoTopic topic) {      
    }

}
