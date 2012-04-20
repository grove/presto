package net.ontopia.presto.jaxrs;

import java.util.Collection;
import java.util.Collections;
import java.util.TreeSet;

import junit.framework.Assert;
import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;

import org.junit.Before;
import org.junit.Test;

public class PrestoTest {
    
    private PrestoDataProvider dataProvider;
    private PrestoSchemaProvider schemaProvider;

    @Before 
    public void setUp() {
        String databaseId = "test";
        this.dataProvider = PrestoTestService.createDataProvider(databaseId);
        this.schemaProvider = PrestoTestService.createSchemaProvider(databaseId);
    }
    
    @Before 
    public void tearDown() {
        this.dataProvider = null;
        this.schemaProvider = null;
    }
    
    @Test 
    public void testCreateAndLookup() {
        PrestoType personType = schemaProvider.getTypeById("person");
        PrestoField nameField = personType.getFieldById("name");
        
        // create new person
        PrestoChangeSet cs = dataProvider.newChangeSet();
        
        PrestoUpdate john = cs.createTopic(personType);
        Collection<? extends Object> names =  Collections.singleton("John Doe");
        john.setValues(nameField, names);        
        cs.save();
        
        PrestoTopic createdJohn = john.getTopicAfterUpdate();
        Assert.assertNotNull(createdJohn);
        
        // lookup person
        PrestoTopic foundJohn = dataProvider.getTopicById(createdJohn.getId());
        Assert.assertNotNull(createdJohn);

        Assert.assertEquals(foundJohn.getId(), createdJohn.getId());
        Assert.assertEquals(foundJohn.getName(), createdJohn.getName());

        Assert.assertEquals(foundJohn.getTypeId(), createdJohn.getTypeId());
        Assert.assertEquals(foundJohn.getTypeId(), personType.getId());

        assertCollectionsEqual(foundJohn.getValues(nameField), createdJohn.getValues(nameField));
        assertCollectionsEqual(foundJohn.getValues(nameField), names);
    }

    private void assertCollectionsEqual(Collection<? extends Object> c1, Collection<? extends Object> c2) {
        Assert.assertEquals(new TreeSet<Object>(c1), new TreeSet<Object>(c2));
    }
    
}
