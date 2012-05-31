package net.ontopia.presto.jaxrs;

import java.util.Arrays;
import java.util.Collection;
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
    public void testCreateLookupAndDelete() {
        PrestoType personType = schemaProvider.getTypeById("person");
        
        // create new person
        PrestoChangeSet cs = dataProvider.newChangeSet(null);
        PrestoUpdate john = cs.createTopic(personType);
        
        // name
        PrestoField nameField = personType.getFieldById("name");
        Collection<? extends Object> names =  Arrays.asList("John Doe");
        john.setValues(nameField, names);        

        // set values and unicode characters
        PrestoField interestsField = personType.getFieldById("interests");
        Collection<? extends Object> interests =  Arrays.asList("Beer", "Wine", "Food", "苹果酒");
        john.setValues(interestsField, interests);        

        // set, remove and add
        PrestoField colorsField = personType.getFieldById("favorite-colors");
        Collection<? extends Object> colors =  Arrays.asList("Green", "Black", "Blue", "Red");
        Collection<? extends Object> removedColors =  Arrays.asList("Blue", "Green");
        Collection<? extends Object> addedColors =  Arrays.asList("White", "Yellow");
        Collection<? extends Object> finalColors =  Arrays.asList("Black", "Red", "White", "Yellow");
        john.setValues(colorsField, colors);        
        john.removeValues(colorsField, removedColors);
        john.addValues(colorsField, addedColors);
        
        cs.save();
        
        PrestoTopic createdJohn = john.getTopicAfterSave();
        Assert.assertNotNull(createdJohn);
        
        // lookup person
        PrestoTopic foundJohn = dataProvider.getTopicById(createdJohn.getId());
        Assert.assertNotNull(createdJohn);

        Assert.assertEquals(foundJohn.getId(), createdJohn.getId());
        Assert.assertEquals(foundJohn.getName(), createdJohn.getName());

        Assert.assertEquals(foundJohn.getTypeId(), personType.getId());
        Assert.assertEquals(foundJohn.getTypeId(), createdJohn.getTypeId());

        compareFieldValues(nameField, names, createdJohn, foundJohn);
        compareFieldValues(interestsField, interests, createdJohn, foundJohn);
        compareFieldValues(colorsField, finalColors, createdJohn, foundJohn);

        // delete person
        cs = dataProvider.newChangeSet(null);

        cs.deleteTopic(foundJohn, personType);
        cs.save();
        
        PrestoTopic deletedJohn = dataProvider.getTopicById(foundJohn.getId());
        Assert.assertNull(deletedJohn);
    }

    private void compareFieldValues(PrestoField field, Collection<? extends Object> values, 
            PrestoTopic t1, PrestoTopic t2) {
        assertCollectionsEqual(t2.getValues(field), t1.getValues(field));
        assertCollectionsEqual(t2.getValues(field), values);
    }

    private void assertCollectionsEqual(Collection<? extends Object> c1, Collection<? extends Object> c2) {
        Assert.assertEquals(new TreeSet<Object>(c1), new TreeSet<Object>(c2));
    }
    
}