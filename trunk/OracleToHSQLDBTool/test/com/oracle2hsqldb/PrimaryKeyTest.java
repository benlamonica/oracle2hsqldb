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
public class PrimaryKeyTest extends TestCase {
    public void testAddColumnWithWrongOwnerThrowsException() {
        try {
            PrimaryKey pk = new PrimaryKey();
            pk.name("PK_HMM");
            Table t = new Table("T_BLAH", Table.Type.TABLE);
            Column c1 = new Column("HMM", 1, 0, 0, true);
            t.addColumn(c1);
            pk.addColumn(c1);
            pk.addColumn(new Column("HOHO", 1, 0, 0, true));
            fail("no exception thrown");
        } catch (IllegalArgumentException e) {
            assertEquals("column to add does not have the same owner table as existing primary key columns", e.getMessage());
        }
    }

    public void testAddingColumnTwiceOnlyAddsItOnce() {
        PrimaryKey pk = new PrimaryKey();
        pk.name("PK_HMM");
        Table t = new Table("T_BLAH", Table.Type.TABLE);
        Column c1 = new Column("HMM", 1, 0, 0, true);
        t.addColumn(c1);
        pk.addColumn(c1);
        pk.addColumn(c1);
        assertEquals(1, pk.columns().size());
    }
}
