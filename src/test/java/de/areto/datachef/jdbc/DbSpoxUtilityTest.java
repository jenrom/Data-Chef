package de.areto.datachef.jdbc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DbSpoxUtilityTest {

    @Test
    public void testParseSqlType() {
        String a = "decimal(16,2)";
        String a2 = " decimal ( 16 , 2) ";
        String b = "date";
        String c = "varchar(50)";

        String[] aParts = DbSpoxUtility.parseSqlType(a);
        assertNotNull(aParts);
        assertEquals(3, aParts.length);
        assertEquals("decimal", aParts[0]);
        assertEquals("16", aParts[1]);
        assertEquals("2", aParts[2]);

        String[] bParts = DbSpoxUtility.parseSqlType(b);
        assertNotNull(bParts);
        assertEquals("date", bParts[0]);
        assertEquals(null, bParts[1]);
        assertEquals(null, bParts[2]);

        String[] cParts = DbSpoxUtility.parseSqlType(c);
        assertNotNull(cParts);
        assertEquals("varchar", cParts[0]);
        assertEquals("50", cParts[1]);
        assertEquals(null, cParts[2]);

        String[] a2Parts = DbSpoxUtility.parseSqlType(a2);
        assertNotNull(a2Parts);
        assertEquals(3, a2Parts.length);
        assertEquals("decimal", a2Parts[0]);
        assertEquals("16", a2Parts[1]);
        assertEquals("2", a2Parts[2]);
    }

    @Test
    public void testEscapeComment() throws Exception {
        String test = "clc=\"to_''date''(bla, 'dd.mm.yy'test'test')\"";
        String target = "clc=\"to_''date''(bla, ''dd.mm.yy''test''test'')\"";
        assertEquals(target, DbSpoxUtility.escapeSingleQuotes(test));
    }

}
