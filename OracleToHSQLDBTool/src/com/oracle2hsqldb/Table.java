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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Moses Hohman
 */
public class Table {
    private String name;
    private PrimaryKey primaryKey;
    private List columns = new LinkedList();
    private Map constraints = new HashMap();
    private Type type;

    public Table(String name) {
        this(name, Type.TABLE);
    }

    protected Table(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    public String name() {
        return name;
    }

    public Type type() {
        return type;
    }

    public List columns() {
        return Collections.unmodifiableList(columns);
    }

    public Column findColumn(String name) {
        Iterator cols = columns.iterator();
        while (cols.hasNext()) {
            Column col = (Column) cols.next();
            if (col.name().equals(name)) {
                return col;
            }
        }
        return null;
    }

    public void addColumn(Column column) {
        columns.add(column);
        column.owner(this);
    }

    public void primaryKey(PrimaryKey pk) {
        Iterator pkColumns = pk.columns().iterator();
        while (pkColumns.hasNext()) {
            if (!columns.contains(pkColumns.next())) {
                throw new IllegalArgumentException("No such column");
            }
        }
        this.primaryKey = pk;
    }

    public PrimaryKey primaryKey() {
        return primaryKey;
    }

    public String toString() {
        StringBuffer result = new StringBuffer("[Table ").append(name).append(":").append(System.getProperty("line.separator"));
        for(Iterator cols = columns.iterator(); cols.hasNext(); ) {
            result.append("\t").append(cols.next()).append(System.getProperty("line.separator"));
        }
        return result.append("]").toString();
    }

    public UniqueConstraint findConstraint(String name) {
        return (UniqueConstraint) constraints.get(name);
    }

    public void addConstraint(UniqueConstraint constraint) {
        constraints.put(constraint.name(), constraint);
    }

    public List constraints() {
        return Collections.unmodifiableList(new ArrayList(constraints.values()));
    }

    public void removeConstraint(UniqueConstraint constraint) {
        constraints.remove(constraint.name());
    }

    public List constraintsFor(Column column) {
        Iterator cons = constraints().iterator();
        List result = new LinkedList();
        while (cons.hasNext()) {
            UniqueConstraint constraint = (UniqueConstraint) cons.next();
            if (constraint.columns().contains(column)) {
                result.add(constraint);
            }
        }
        return result;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Table)) return false;

        final Table table = (Table) o;

        if (name != null ? !name.equals(table.name) : table.name != null) return false;

        return true;
    }

    public int hashCode() {
        return (name != null ? name.hashCode() : 0);
    }

    public static class Type {
        private static Map byName = new HashMap();
        public static final Type TABLE = new Type("TABLE");
        public static final Type VIEW = new Type("VIEW");

        public static String[] getSupportedNames() {
            return new String[] { TABLE.getJdbcName(), VIEW.getJdbcName() };
        }

        public static Type getByJdbcName(String jdbcName) {
            return (Type) byName.get(jdbcName);
        }

        private static void register(Type t) {
            byName.put(t.getJdbcName(), t);
        }

        private final String typeStr;
        private Type(String str) {
            this.typeStr = str;
            register(this);
        }

        public String getJdbcName() {
            return typeStr;
        }

        public String toString() {
            return getJdbcName();
        }
    }

    public static class Spec {
        private String tableName;
        private Table.Type tableType;

        public Spec(String tableName, String tableTypeName) {
            this.tableName = tableName;
            this.tableType = Table.Type.getByJdbcName(tableTypeName);
        }

        public String getTableName() {
            return tableName;
        }

        public Table.Type getTableType() {
            return tableType;
        }

        public Table getTable() {
            return new Table(getTableName(), getTableType());
        }
    }
}
