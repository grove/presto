package net.ontopia.presto.spi.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class UtilsTest {

    @Test
    public void moveValueToIndex() {
        List<String> values = Arrays.asList("1", "2", "3");
        List<String> moveValues = Arrays.asList("2");
        List<String> expected = Arrays.asList("1", "3", "2");
        
        boolean allowAdd = false;
        assertEquals(expected, Utils.moveValuesToIndex(values, moveValues, 3, allowAdd));
    }

    @Test
    public void moveValuesToIndex() {
        List<String> values = Arrays.asList("1", "2", "3", "4");
        List<String> moveValues = Arrays.asList("1", "2");
        List<String> expected = Arrays.asList("3", "1", "2", "4");
        
        boolean allowAdd = false;
        assertEquals(expected, Utils.moveValuesToIndex(values, moveValues, 3, allowAdd));
    }

    @Test
    public void moveValuesToSameIndex() {
        List<String> values = Arrays.asList("1", "2", "3", "4");
        List<String> moveValues = Arrays.asList("1", "2");
        List<String> expected = Arrays.asList("1", "2", "3", "4");
        
        boolean allowAdd = false;
        assertEquals(expected, Utils.moveValuesToIndex(values, moveValues, 0, allowAdd));
    }

    @Test
    public void moveValuesToLastIndex() {
        List<String> values = Arrays.asList("1", "2", "3", "4");
        List<String> moveValues = Arrays.asList("1", "2");
        List<String> expected = Arrays.asList("3", "4", "1", "2");
        
        boolean allowAdd = false;
        assertEquals(expected, Utils.moveValuesToIndex(values, moveValues, values.size(), allowAdd));
    }

    @Test
    public void moveValuesToOutOfBoundsIndex() {
        List<String> values = Arrays.asList("1", "2", "3", "4");
        List<String> moveValues = Arrays.asList("1", "2");
        
        boolean allowAdd = false;
        try {
            Utils.moveValuesToIndex(values, moveValues, values.size() + 5, allowAdd);
            fail("Was allowed to move values out of bounds");
        } catch (RuntimeException e) {
            assertEquals("Index: 9, Size: 4", e.getMessage());
        }
    }

    @Test
    public void moveNoneToIndex() {
        List<String> values = Arrays.asList("1", "2", "3");
        List<String> moveValues = Arrays.asList();
        List<String> expected = Arrays.asList("1", "2", "3");
        
        boolean allowAdd = false;
        assertEquals(expected, Utils.moveValuesToIndex(values, moveValues, 2, allowAdd));
    }

    @Test
    public void moveAllToIndex() {
        List<String> values = Arrays.asList("1", "2", "3");
        List<String> moveValues = Arrays.asList("1", "2", "3");
        List<String> expected = Arrays.asList("1", "2", "3");
        
        boolean allowAdd = false;
        assertEquals(expected, Utils.moveValuesToIndex(values, moveValues, 2, allowAdd));
    }

    @Test
    public void moveUnknownToIndex() {
        List<String> values = Arrays.asList("1", "2", "3");
        List<String> moveValues = Arrays.asList("4");
        
        boolean allowAdd = false;
        try {
            Utils.moveValuesToIndex(values, moveValues, 2, allowAdd);
            fail("Was allowed to add new value: 4");
        } catch (RuntimeException e) {
            assertEquals("Not allowed to add new values: 4", e.getMessage());
        }
    }

    @Test
    public void moveUnknownToIndexAllowAdd() {
        List<String> values = Arrays.asList("1", "2", "3");
        List<String> moveValues = Arrays.asList("4");
        List<String> expected = Arrays.asList("1", "2", "4", "3");
        
        boolean allowAdd = true;
        assertEquals(expected, Utils.moveValuesToIndex(values, moveValues, 2, allowAdd));
    }

    @Test
    public void moveUnknownsToIndex() {
        List<String> values = Arrays.asList("1", "2", "3");
        List<String> moveValues = Arrays.asList("4", "5");
        
        boolean allowAdd = false;
        try {
            Utils.moveValuesToIndex(values, moveValues, 2, allowAdd);
            fail("Was allowed to add new value: 4");
        } catch (RuntimeException e) {
            assertEquals("Not allowed to add new values: 4", e.getMessage());
        }
    }

    @Test
    public void moveUnknownsToIndexAllowAdd() {
        List<String> values = Arrays.asList("1", "2", "3");
        List<String> moveValues = Arrays.asList("4", "5");
        List<String> expected = Arrays.asList("1", "2", "4", "5", "3");
        
        boolean allowAdd = true;
        assertEquals(expected, Utils.moveValuesToIndex(values, moveValues, 2, allowAdd));
    }

}
