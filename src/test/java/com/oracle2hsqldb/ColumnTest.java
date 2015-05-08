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
import com.oracle2hsqldb.Index;

import junit.framework.TestCase;

/**
 * @author Moses Hohman
 */
public class ColumnTest extends TestCase {

    public void testIsPrimaryKeyWhenTrue() {
        Column column = new Column("", 1, 0, 0, false);
        Table table = new Table("", Table.Type.TABLE);
        table.addColumn(column);
        PrimaryKey pk = new PrimaryKey();
        pk.addColumn(column);
        table.primaryKey(pk);
        assertTrue("is not member", column.isPrimaryKeyMember());
        assertTrue("is not key", column.isPrimaryKey());
    }

    public void testIsPrimaryKeyWhenTrueAndMultipleMembers() {
        Column column1 = new Column("a", 1, 0, 0, false);
        Column column2 = new Column("b", 1, 0, 0, false);
        Table table = new Table("", Table.Type.TABLE);
        table.addColumn(column1);
        table.addColumn(column2);
        PrimaryKey pk = new PrimaryKey();
        pk.addColumn(column1);
        pk.addColumn(column2);
        table.primaryKey(pk);
        assertTrue("column 1 is not member", column1.isPrimaryKeyMember());
        assertFalse("column 1 is key", column1.isPrimaryKey());
        assertTrue("column 2 is not member", column2.isPrimaryKeyMember());
        assertFalse("column 2 is key", column2.isPrimaryKey());
    }

    public void testIsPrimaryKeyWhenFalse() {
        Column column = new Column("", 1, 0, 0, false);
        Table table = new Table("", Table.Type.TABLE);
        table.addColumn(column);
        assertFalse("is member", column.isPrimaryKeyMember());
        assertFalse("is key", column.isPrimaryKey());
    }

    public void testIndexBy() {
        Column column = new Column("id", 1, 0, 0, false);
        Table table = new Table("t", Table.Type.TABLE);
        table.addColumn(column);

        Index constraint = new Index("uk", true);
        column.indexedBy(constraint);

        assertEquals("constraint not added to table", constraint, table.findIndex("uk"));
        assertEquals("column constraints wrong size", 1, column.uniqueConstraints().size());
        assertEquals("constraint not added to column", constraint, column.uniqueConstraints().get(0));
        assertEquals("unique key columns wrong size", 1, constraint.columns().size());
        assertEquals("unique key does not contain column", column, constraint.columns().get(0));

        assertTrue("column not unique key member", column.isUniqueKeyMember());
        assertTrue("column not unique", column.isUnique());
    }

    public void testConstrainByMustBeCalledAfterColumnAddedToTable() {
        try {
            new Column("id", 1, 0, 0, false).indexedBy(new Index("whatever", true));
        } catch (IllegalStateException e) {
            assertEquals("column must be added to table before calling constrainBy()", e.getMessage());
        }
    }
}
