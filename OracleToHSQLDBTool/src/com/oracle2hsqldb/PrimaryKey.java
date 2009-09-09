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

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @author Moses Hohman
 */
public class PrimaryKey {
    private String name;
    private Set columns = new HashSet();

    public String name() {
        return name;
    }

    public void name(String name) {
        this.name = name;
    }

    public void addColumn(Column column) {
        if (columns.size() > 0) {
            Column existingColumn = (Column) columns.iterator().next();
            if (!existingColumn.owner().equals(column.owner())) {
                throw new IllegalArgumentException("column to add does not have the same owner table as existing primary key columns");
            }
        }
        columns.add(column);
    }

    public List columns() {
        return new LinkedList(columns);
    }

    public String toString() {
        return name;
    }

    public static class Spec {
        private String tableName;
        private String name;
        private List columnNames;

        public Spec(String tableName, String name) {
            this.tableName = tableName;
            this.name = name;
            this.columnNames = new LinkedList();
        }

        public void addColumnName(String name) {
            columnNames.add(name);
        }

        public void addPrimaryKey(Table table) {
            PrimaryKey key = new PrimaryKey();
            key.name(this.name);

            for (Iterator columnNames = this.columnNames.iterator(); columnNames.hasNext(); ) {
                String columnName = (String) columnNames.next();
                key.addColumn(table.findColumn(columnName));
            }

            table.primaryKey(key);
        }

        public String getTableName() {
            return tableName;
        }
    }
}
