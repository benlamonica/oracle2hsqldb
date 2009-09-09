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

import com.oracle2hsqldb.Schema;
import com.oracle2hsqldb.Table;

import junit.framework.TestCase;

/**
 * @author Moses Hohman
 */
public class SchemaTest extends TestCase {
    public void testFindTable() {
        Schema schema = new Schema("hi");
        Table t = new Table("t_t", Table.Type.TABLE);
        schema.addTable(t);
        assertEquals(t, schema.findTable("t_t"));
    }
}
