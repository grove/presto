package net.ontopia.presto.jaxrs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import net.ontopia.presto.jaxb.AvailableFieldTypes;
import net.ontopia.presto.jaxb.AvailableFieldValues;
import net.ontopia.presto.jaxb.AvailableTopicMaps;
import net.ontopia.presto.jaxb.AvailableTopicTypes;
import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.jaxb.RootInfo;
import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxb.TopicMap;
import net.ontopia.presto.jaxb.TopicType;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoSession;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;

@Path("/editor")
public abstract class EditorResource {
    
  public final static String APPLICATION_JSON_UTF8 = "application/json;charset=UTF-8";

  // TODO: add more endpoints: 
  //
  // 1: / - information about server and link to /available-topicmaps
  // 2: /available-topicmaps - lists available topic maps
  // 3: /create-instance/{topicMapId}

  @GET
  @Produces(APPLICATION_JSON_UTF8)
  public Response getRootInfo(@Context UriInfo uriInfo) throws Exception {

    RootInfo result = new RootInfo();

    result.setId(uriInfo.getBaseUri() + "editor");
    result.setVersion(0);
    result.setName("Presto - Editor REST API");

    List<Link> links = new ArrayList<Link>();
    links.add(new Link("available-topicmaps", uriInfo.getBaseUri() + "editor/available-topicmaps"));
    result.setLinks(links);      

    return Response.ok(result).build();
  }

  @GET
  @Produces(APPLICATION_JSON_UTF8)
  @Path("available-topicmaps")
  public Response getTopicMaps(@Context UriInfo uriInfo) throws Exception {

    AvailableTopicMaps result = new AvailableTopicMaps();

    result.setId("topicmaps");
    result.setName("Presto - Editor REST API");

    Collection<TopicMap> topicmaps = new ArrayList<TopicMap>();
    
    TopicMap topicmap = new TopicMap();
    topicmap.setId("litteraturklubben.xtm");
    topicmap.setName("Litteraturklubben");

    List<Link> links = new ArrayList<Link>();
    links.add(new Link("edit", uriInfo.getBaseUri() + "editor/topicmap-info/" + topicmap.getId()));
    topicmap.setLinks(links);    
    
    topicmaps.add(topicmap);
    result.setTopicMaps(topicmaps);      

    return Response.ok(result).build();
  }

  @GET
  @Produces(APPLICATION_JSON_UTF8)
  @Path("topicmap-info/{topicMapId}")
  public Response getTopicMapInfo(
      @Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId) throws Exception {

    PrestoSession session = createSession(topicMapId);
    try {
      TopicMap result = new TopicMap();

      result.setId(session.getDatabaseId());
      result.setName(session.getDatabaseName());

      List<Link> links = new ArrayList<Link>();
      links.add(new Link("available-types-tree", uriInfo.getBaseUri() + "editor/available-types-tree/" + session.getDatabaseId()));
      links.add(new Link("available-types-tree-lazy", uriInfo.getBaseUri() + "editor/available-types-tree-lazy/" + session.getDatabaseId()));
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
  @Produces(APPLICATION_JSON_UTF8)
  @Path("create-instance/{topicMapId}/{typeId}")
  public Response createInstance(
      @Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId, 
      @PathParam("typeId") final String typeId) throws Exception {

    PrestoSession session = createSession(topicMapId);
    PrestoSchemaProvider schemaProvider = session.getSchemaProvider();

    try {

      PrestoType type = schemaProvider.getTypeById(typeId);
      if (type == null) {
        return Response.status(Status.NOT_FOUND).build();
      }
      PrestoView view = type.getDefaultView();

      Topic result = postProcess(Utils.getNewTopicInfo(uriInfo, type, view));
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
  @Path("create-field-instance/{topicMapId}/{parentTopicId}/{parentFieldId}/{playerTypeId}")
  public Response createFieldInstance(
      @Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId,
      @PathParam("parentTopicId") final String parentTopicId,
      @PathParam("parentFieldId") final String parentFieldId, 
      @PathParam("playerTypeId") final String playerTypeId) throws Exception {

    PrestoSession session = createSession(topicMapId);
    PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
    
    try {

      PrestoType type = schemaProvider.getTypeById(playerTypeId);
      if (type == null) {
        return Response.status(Status.NOT_FOUND).build();
      }
      PrestoView view = type.getDefaultView();

      Topic result = postProcess(Utils.getNewTopicInfo(uriInfo, type, view, parentTopicId, parentFieldId));
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
  @Path("topic-data/{topicMapId}/{topicId}")
  public Response getTopicData(
      @Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId, 
      @PathParam("topicId") final String topicId) throws Exception {

    PrestoSession session = createSession(topicMapId);
    PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
    PrestoDataProvider dataProvider = session.getDataProvider();
    
    try {

      PrestoTopic topic = dataProvider.getTopicById(topicId);
      if (topic == null) {
        return Response.status(Status.NOT_FOUND).build();
      }
      PrestoType type = schemaProvider.getTypeById(topic.getTypeId());
      
      Map<String,Object> result = Utils.getTopicData(uriInfo, topic, type);
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
  @Path("topic/{topicMapId}/{topicId}")
  public Response deleteTopic(
      @Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId, 
      @PathParam("topicId") final String topicId) throws Exception {

    PrestoSession session = createSession(topicMapId);
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
        if (Utils.deleteTopic(uriInfo, topic, type, dataProvider)) {          
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
  @Produces(APPLICATION_JSON_UTF8)
  @Path("topic/{topicMapId}/{topicId}")
  public Response getTopicInDefaultView(
      @Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId, 
      @PathParam("topicId") final String topicId,
      @QueryParam("readOnly") final boolean readOnly) throws Exception {

    PrestoSession session = createSession(topicMapId);
    PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
    PrestoDataProvider dataProvider = session.getDataProvider();

    try {

      PrestoTopic topic = dataProvider.getTopicById(topicId);
      if (topic == null) {
          return Response.status(Status.NOT_FOUND).build();
        }
      PrestoType type = schemaProvider.getTypeById(topic.getTypeId());
      PrestoView view = type.getDefaultView();
      
      Topic result = postProcess(Utils.getTopicInfo(uriInfo, topic, type, view, readOnly));
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
  @Path("topic/{topicMapId}/{topicId}/{viewId}")
  public Response getTopicInView(
      @Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId, 
      @PathParam("topicId") final String topicId,
      @PathParam("viewId") final String viewId,
      @QueryParam("readOnly") final boolean readOnly) throws Exception {

    PrestoSession session = createSession(topicMapId);
    PrestoSchemaProvider schemaProvider = session.getSchemaProvider();
    PrestoDataProvider dataProvider = session.getDataProvider();

    try {

      PrestoTopic topic = dataProvider.getTopicById(topicId);
      if (topic == null) {
        return Response.status(Status.NOT_FOUND).build();
      }
      PrestoType type = schemaProvider.getTypeById(topic.getTypeId());
      PrestoView view = type.getViewById(viewId);

      Topic result = postProcess(Utils.getTopicInfo(uriInfo, topic, type, view, readOnly));
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
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("topic/{topicMapId}/{topicId}/{viewId}")
  public Response updateTopic(@Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId, 
      @PathParam("topicId") final String topicId, 
      @PathParam("viewId") final String viewId, Topic topicData) throws Exception {

    PrestoSession session = createSession(topicMapId);
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
      
      topic = Utils.updateTopic(uriInfo, session, topic, type, view, preProcess(topicData));
      
      Topic result = Utils.getTopicInfo(uriInfo, topic, type, view, false);
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
  @Produces(APPLICATION_JSON_UTF8)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("add-field-values-at-index/{topicMapId}/{topicId}/{viewId}/{fieldId}/{index}")
  public Response addFieldValuesAtIndex(@Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId, 
      @PathParam("topicId") final String topicId, 
      @PathParam("viewId") final String viewId,
      @PathParam("fieldId") final String fieldId, 
      @PathParam("index") final Integer index, FieldData fieldData) throws Exception {

      PrestoSession session = createSession(topicMapId);
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

        FieldData result = Utils.addFieldValues(uriInfo, session, topic, field, index, fieldData);

        String id = topic.getId();

        session.commit();
        onTopicUpdated(id);

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
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("move-field-values-to-index/{topicMapId}/{topicId}/{viewId}/{fieldId}/{index}")
  public Response moveFieldValuesToIndex(@Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId, 
      @PathParam("topicId") final String topicId, 
      @PathParam("viewId") final String viewId,
      @PathParam("fieldId") final String fieldId, 
      @PathParam("index") final Integer index, FieldData fieldData) throws Exception {
      
      return addFieldValuesAtIndex(uriInfo, topicMapId, topicId, viewId, fieldId, index, fieldData);
  }
  
  @POST
  @Produces(APPLICATION_JSON_UTF8)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("add-field-values/{topicMapId}/{topicId}/{viewId}/{fieldId}")
  public Response addFieldValues(@Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId, 
      @PathParam("topicId") final String topicId, 
      @PathParam("viewId") final String viewId,
      @PathParam("fieldId") final String fieldId, FieldData fieldData) throws Exception {

      Integer index = null;
      return addFieldValuesAtIndex(uriInfo, topicMapId, topicId, viewId, fieldId, index, fieldData);
  }

  @POST
  @Produces(APPLICATION_JSON_UTF8)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("remove-field-values/{topicMapId}/{topicId}/{viewId}/{fieldId}")
  public Response removeFieldValues(@Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId, 
      @PathParam("topicId") final String topicId, 
      @PathParam("viewId") final String viewId,
      @PathParam("fieldId") final String fieldId, FieldData fieldData) throws Exception {

    PrestoSession session = createSession(topicMapId);
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

      FieldData result =  Utils.removeFieldValues(uriInfo, session, topic, field, fieldData);

      String id = topic.getId();

      session.commit();
      onTopicUpdated(id);

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
  @Path("available-field-values/{topicMapId}/{topicId}/{viewId}/{fieldId}")
  public Response getAvailableFieldValues(@Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId, 
      @PathParam("topicId") final String topicId, 
      @PathParam("viewId") final String viewId,
      @PathParam("fieldId") final String fieldId) throws Exception {

    PrestoSession session = createSession(topicMapId);
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
      AvailableFieldValues result = createFieldInfoAllowed(uriInfo, field, availableFieldValues);
      
      return Response.ok(result).build();

    } catch (Exception e) {
      session.abort();
      throw e;
    } finally {
      session.close();      
    }
  }

  private AvailableFieldValues createFieldInfoAllowed(UriInfo uriInfo, PrestoFieldUsage field, Collection<PrestoTopic> availableFieldValues) {

    AvailableFieldValues result = new AvailableFieldValues();
    result.setId(field.getId());
    result.setName(field.getName());

    List<Value> values = new ArrayList<Value>(availableFieldValues.size());
    if (!availableFieldValues.isEmpty()) {
      
      PrestoView valueView = field.getValueView();
      boolean traversable = field.isTraversable();
      
      for (PrestoTopic value : availableFieldValues) {
        values.add(Utils.getAllowedTopicFieldValue(uriInfo, value, valueView, traversable));
      }
    } 
    result.setValues(values);

    return result;
  }

  @GET
  @Produces(APPLICATION_JSON_UTF8)
  @Path("available-field-types/{topicMapId}/{topicId}/{viewId}/{fieldId}")
  public Response getAvailableFieldTypes(@Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId, 
      @PathParam("topicId") final String topicId, 
      @PathParam("viewId") final String viewId,
      @PathParam("fieldId") final String fieldId) throws Exception {

    PrestoSession session = createSession(topicMapId);
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
      
      Collection<PrestoType> availableFieldCreateTypes = field.getAvailableFieldCreateTypes();

      List<TopicType> types = new ArrayList<TopicType>(availableFieldCreateTypes.size());
      for (PrestoType createType : availableFieldCreateTypes) {
        types.add(Utils.getCreateFieldInstance(uriInfo, topic, type, field, createType));
      }
      result.setTypes(types);
      
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
  @Path("available-types-tree-lazy/{topicMapId}")
  public Response getAvailableTypesTreeLazy(@Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId) throws Exception {

    PrestoSession session = createSession(topicMapId);
    PrestoSchemaProvider schemaProvider = session.getSchemaProvider();

    try {

      AvailableTopicTypes result = new AvailableTopicTypes();
      result.setTypes(TypeUtils.getAvailableTypes(uriInfo, schemaProvider.getRootTypes(), false));      

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
  @Path("available-types-tree-lazy/{topicMapId}/{typeId}")
  public Response getAvailableTypesTreeLazy(@Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId, 
      @PathParam("typeId") final String typeId) throws Exception {

    PrestoSession session = createSession(topicMapId);
    PrestoSchemaProvider schemaProvider = session.getSchemaProvider();

    try {
      PrestoType type = schemaProvider.getTypeById(typeId);

      AvailableTopicTypes result = new AvailableTopicTypes();
      result.setTypes(TypeUtils.getAvailableTypes(uriInfo, type.getDirectSubTypes(), false));      

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
  @Path("available-types-tree/{topicMapId}")
  public Response getAvailableTypesTree(@Context UriInfo uriInfo, 
      @PathParam("topicMapId") final String topicMapId) throws Exception {

    PrestoSession session = createSession(topicMapId);
    PrestoSchemaProvider schemaProvider = session.getSchemaProvider();

    try {

      AvailableTopicTypes result = new AvailableTopicTypes();
      result.setTypes(TypeUtils.getAvailableTypes(uriInfo, schemaProvider.getRootTypes(), true));      

      return Response.ok(result).build();

    } catch (Exception e) {
      session.abort();
      throw e;
    } finally {
      session.close();      
    }
  }

  // overridable methods
  
  protected abstract PrestoSession createSession(String topicMapId);

  protected void onTopicUpdated(String topicId) {      
  }

  protected Topic preProcess(Topic topicInfo) {
    return topicInfo;
  }

  protected Topic postProcess(Topic topicInfo) {
    return topicInfo;
  }

}
