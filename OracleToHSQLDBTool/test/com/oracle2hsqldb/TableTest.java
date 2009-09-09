/*
 * Schemamule, a library for automating database schema tasks
 * Copyright (C) 2006, Moses M. Hohman and Rhett Sutphin
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St., 5th Floor, Boston, MA  02110-1301

 * To contact the authors, send email to:
 * { mmhohman OR rsutphin } AT sourceforge DOT net
 */

package com.oracle2hsqldb;

import com.oracle2hsqldb.Column;
import com.oracle2hsqldb.PrimaryKey;
import com.oracle2hsqldb.Table;

import junit.framework.TestCase;

/**
 * @author Moses Hohman
 */
public class TableTest extends TestCase {
    public void testFindColumn() {
        Table t = new Table("whatever", Table.Type.TABLE);
        Column columnA = new Column("a", 1, 0, 0, true);
        Column columnB = new Column("b", 1, 0, 0, true);
        t.addColumn(columnA);
        t.addColumn(columnB);
        assertEquals(columnB, t.findColumn("b"));
    }

    public void testAddingNonexistentColumnThrowsException() {
        try {
            Table t = new Table("", Table.Type.TABLE);
            Column c = new Column("blah", 1, 0, 0, true);
            PrimaryKey pk = new PrimaryKey();
            pk.addColumn(c);
            t.primaryKey(pk);
            fail("no exception thrown");
        } catch (IllegalArgumentException e) {
            assertEquals("No such column", e.getMessage());
        }
    }
}
