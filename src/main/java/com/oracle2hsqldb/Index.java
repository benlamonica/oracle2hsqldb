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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Moses Hohman
 */
public class Index {
    private String name;
    private boolean isUnique;
    private List<Column> columns;

    public Index(String name, boolean isUnique) {
        this.name = name;
        this.isUnique = isUnique;
        this.columns = new ArrayList<Column>();
    }

    public String name() {
        return name;
    }
    
    public boolean isUnique() {
    	return isUnique;
    }

    public void name(String name) {
        this.name = name;
    }

    public List<Column> columns() {
        return columns;
    }

    public void addColumn(Column column) {
        boolean isNewColumn = true;
        for (Iterator<Column> cols = columns.iterator(); cols.hasNext();) {
            Column existingColumn = cols.next();
            if (!column.owner().equals(existingColumn.owner())) {
                throw new IllegalStateException("all index columns must have the same owning table");
            }
            if (existingColumn.equals(column)) {
                isNewColumn = false;
            }
        }
        if (isNewColumn) {
            columns.add(column);
        }
    }

    
    public String toString() {
    	return (isUnique() ? "UNIQUE" : "NOT-UNIQUE") + " INDEX " + name() + "[" + columns + "]"; 
    }
    
    public static class Spec {
        private String indexName;
        private String columnName;
        private String tableName;
        private boolean isUnique;
        
        public Spec(String tableName, String columnName, String indexName, boolean isUnique) {
            this.tableName = tableName;
            this.columnName = columnName;
            this.indexName = indexName;
            this.isUnique = isUnique;
        }

        public boolean isUnique() {
        	return isUnique;
        }
        
        public String getIndexName() {
            return indexName;
        }

        public String getColumnName() {
            return columnName;
        }

        public String getTableName() {
            return tableName;
        }
    }
}
