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

import java.util.Iterator;
import java.util.List;

/**
 * @author Moses Hohman
 */
public class SchemaWriter {
    private boolean viewsAsTables = false;
    private Configuration configuration;

    public SchemaWriter(Configuration configuration) {
        this.configuration = configuration;
    }

    public void setViewsAsTables(boolean viewsAsTables) {
        this.viewsAsTables = viewsAsTables;
    }

    public String write(Schema schema) {
    	return "CREATE SCHEMA " + schema.name() + " AUTHORIZATION DBA\n";
    }

    public String switchToSchema(Schema schema) {
    	return "SET SCHEMA " + schema.name() + "\n";
    }

    public String write(Table table) {
        if (!viewsAsTables && table instanceof View && configuration.dialect().supportsViews()) {
            return createView((View) table);
        } else {
            return createTable(table);
        }
    }
    
    private String createTable(Table table) {
        StringBuilder result = new StringBuilder("CREATE TABLE ").append(table.name()).append(" (\n");
        for (Iterator<Column> columns = table.columns().iterator(); columns.hasNext();) {
            Column column = (Column) columns.next();
            result.append("\t").append(write(column));
            if (columns.hasNext() || table.indicies().size() > 0) {
                result.append(",");
            }
            result.append("\n");
        }
        for (Iterator<Index> uniques = table.indicies().iterator(); uniques.hasNext();) {
            Index index = uniques.next();
            // skip non-unique indexes for right now, we will create them later.
            if (!index.isUnique()) {
            	continue;
            }
            result.append("\t");
            if (index.name() != null) {
                result.append("CONSTRAINT ").append(index.name()).append(" ");
            }
            result.append("UNIQUE (");
            result.append(getCommaSeparatedColString(index.columns()));
            result.append(")");
            if (uniques.hasNext()) {
                result.append(",");
            }
            result.append("\n");
        }

        return result.append(")").toString();
    }

    private String createView(View view) {
        StringBuilder result = new StringBuilder();
        result.append("CREATE VIEW ").append(view.name()).append(" AS ").append(view.text());
        return result.toString();
    }

    public String write(Column column) {
        StringBuilder result = new StringBuilder(column.name());
        String typeName = getTypeName(column);
        result.append(" ").append(typeName);
        int size = getSize(column);
        if (size > 0 && typeIsScalable(typeName)) {
            result.append("(").append(size).append(")");
        }
        if (column.defaultValue() != null) {
            result.append(" DEFAULT ");
            appendDefaultValue(column, result);
        }
        boolean alreadyNotNull = false;
        if (configuration.supportsPrimaryKeys()) {
            if (column.isPrimaryKey()) {
                alreadyNotNull = true;
                if (configuration.dialect().supportsIdentityColumns() && "INTEGER".equals(configuration.dialect().getTypeName(column))) {
                    appendIdentityColumnString(result);
                }
                result.append(" PRIMARY KEY");
            }
        }
        if (!alreadyNotNull && !column.isNullable()) {
            result.append(" NOT NULL");
        }
        return result.toString();
    }

    private StringBuilder appendDefaultValue(Column column, StringBuilder result) {
        return result.append(configuration.dialect().formatDefaultValue(column));
    }

    private boolean typeIsScalable(String typeName) {
        return !(typeName.equals("INTEGER") || typeName.equals("BIT") || typeName.equals("TIMESTAMP") || typeName.equals("DATE") || typeName.equals("LONGVARBINARY"));
    }

    private String getTypeName(Column column) {
        return configuration.dialect().getTypeName(column);
    }

    private int getSize(Column column) {
        return configuration.dialect().getSize(column);
    }

    private void appendIdentityColumnString(StringBuilder result) {
        result.append(" ").append(configuration.dialect().getIdentityColumnString());
    }

    public String write(Sequence sequence) {
        if (!configuration.dialect().supportsSequences()) throw new IllegalStateException(configuration.dialect() + " does not support sequences");
        // This syntax is vaild for Oracle and HSQLDB
        StringBuilder result = new StringBuilder();
        result.append("CREATE SEQUENCE ").append(sequence.name());
        if (sequence.value() != null) {
            result.append(" START WITH ").append(sequence.value());
        }
        return result.toString();
    }

    private String getCommaSeparatedColString(List<Column> cols) {
    	StringBuilder result = new StringBuilder();
        for (Column col : cols) {
            if (result.length() != 0) {
                result.append(", ");
            }
            result.append(col.name());
        }
    	
    	return result.toString();
    }
    
	public String write(Index i) {
		if (!i.isUnique()) { // unique indices were created as part of the table definition
			return String.format("CREATE INDEX %s ON %s(%s)", i.name(), i.columns().get(0).owner().name(), getCommaSeparatedColString(i.columns()));
		}

		return "";
	}
}