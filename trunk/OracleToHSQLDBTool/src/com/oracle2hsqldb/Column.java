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

import java.util.*;

/**
 * @author Moses Hohman
 */
public class Column {
    private Table owner;
    private String name;
    private int type;
    private boolean isNullable;
    private int size;
    private int precision;
    private Reference reference;
    private DefaultValue defaultValue;

    public Column(String name, int type, int size, int precision, boolean nullable) {
        this(name, type, size, precision, nullable, null);
    }

    public Column(String name, int type, int size, int precision, boolean nullable, DefaultValue defaultValue) {
        this.name = name;
        this.type = type;
        this.size= size;
        this.precision = precision;
        isNullable = nullable;
        this.defaultValue = defaultValue;
    }

    public String name() {
        return name;
    }

    public int type() {
        return type;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public int size() {
        return size;
    }

    public int precision() {
        return precision;
    }

    public void owner(Table owner) {
        this.owner = owner;
    }

    public Table owner() {
        return owner;
    }

    public void reference(Reference reference) {
        this.reference = reference;
    }

    public Reference reference() {
        return reference;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKeyMember() && owner.primaryKey().columns().size()==1;
    }

    public boolean isPrimaryKeyMember() {
        if (owner==null) return false;
        if (owner.primaryKey()==null) return false;
        return owner.primaryKey().columns().contains(this);
    }

    public boolean isUnique() {
        if (!isUniqueKeyMember()) return false;
        for(Iterator constraints = uniqueConstraints().iterator(); constraints.hasNext();) {
            UniqueConstraint uniqueConstraint = (UniqueConstraint) constraints.next();
            if (uniqueConstraint.columns().size()==1) {
                return true;
            }
        }
        return false;
    }

    public String toString() {
        StringBuffer result = new StringBuffer("[Column ");
        if (owner!=null) {
            result.append(owner.name()).append(".");
        }
        result.append(name).append("]");
        return result.toString();
    }

    public DefaultValue defaultValue() {
        return defaultValue;
    }

    public boolean isUniqueKeyMember() {
        return uniqueConstraints().size() > 0;
    }

    public List uniqueConstraints() {
        return owner().constraintsFor(this);
    }

    public void constrainBy(UniqueConstraint uniqueConstraint) {
        if (owner()==null) throw new IllegalStateException("column must be added to table before calling constrainBy()");
        UniqueConstraint tableConstraint = owner().findConstraint(uniqueConstraint.name());
        if (tableConstraint==null) {
            owner().addConstraint(uniqueConstraint);
            tableConstraint = uniqueConstraint;
        }
        tableConstraint.addColumn(this);
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Column)) return false;

        final Column column = (Column) o;

        if (name != null ? !name.equals(column.name) : column.name != null) return false;
        if (owner != null ? !owner.equals(column.owner) : column.owner != null) return false;

        return true;
    }

    public int hashCode() {
        int result;
        result = (owner != null ? owner.hashCode() : 0);
        result = 29 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    public static class Spec {
        private String tableName;
        private Column column;

        public Spec(String tableName, Column column) {
            this.tableName = tableName;
            this.column = column;
        }

        public Column getColumn() {
            return column;
        }

        public String getTableName() {
            return tableName;
        }
    }
}