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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;

/**
 * Container for methods helpful during debugging.
 * @author Moses Hohman
 */
public class DebugHelper {
    private DebugHelper() { }
    
    public static void dump(ResultSet set) throws SQLException {
        ResultSetMetaData data = set.getMetaData();
        System.err.println();
        while (set.next()) {
            for (int i = 1; i <= data.getColumnCount(); i++) {
                System.err.print(data.getColumnName(i));
                System.err.print(":");
                System.err.println(set.getObject(i));
            }
            System.err.println();
        }
    }

}
