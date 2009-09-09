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

import junit.framework.TestCase;

import java.util.Calendar;

import com.oracle2hsqldb.HsqldbLibrary;

/**
 * @author Moses Hohman
 */
public class HsqldbLibraryTest extends TestCase {
    public void testNvlWhenNull() {
        assertEquals("default", HsqldbLibrary.nvl(null, "default"));
    }

    public void testNvlWhenNotNull() {
        assertEquals("notnull", HsqldbLibrary.nvl("notnull", "default"));
    }

    public void testTruncDate() {
        Calendar cal = Calendar.getInstance();
        cal.set(2000, Calendar.MARCH, 9, 10, 50, 14);
        Calendar truncd = Calendar.getInstance();
        truncd.setTime(HsqldbLibrary.trunc(cal.getTime()));
        assertEquals("Hour not blanked", 0, truncd.get(Calendar.HOUR_OF_DAY));
        assertEquals("Minute not blanked", 0, truncd.get(Calendar.MINUTE));
        assertEquals("Second not blanked", 0, truncd.get(Calendar.SECOND));
        assertEquals("Wrong year", 2000, truncd.get(Calendar.YEAR));
        assertEquals("Wrong month", Calendar.MARCH, truncd.get(Calendar.MONTH));
        assertEquals("Wrong day", 9, truncd.get(Calendar.DAY_OF_MONTH));
    }

    public void testTruncNull() {
        assertNull(HsqldbLibrary.trunc(null));
    }
}
