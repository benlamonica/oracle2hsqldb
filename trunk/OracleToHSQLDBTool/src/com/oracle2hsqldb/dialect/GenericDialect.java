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

package com.oracle2hsqldb.dialect;

import net.sf.hibernate.MappingException;
import org.springframework.jdbc.core.RowCallbackHandler;

import com.oracle2hsqldb.Column;
import com.oracle2hsqldb.DefaultValue;
import com.oracle2hsqldb.PrimaryKey;
import com.oracle2hsqldb.SchemaException;
import com.oracle2hsqldb.Table;
import com.oracle2hsqldb.UniqueConstraint;
import com.oracle2hsqldb.spring.MetaDataJdbcTemplate;

import javax.sql.DataSource;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

/**
 * @author Moses Hohman
 */
public class GenericDialect implements Dialect {
    private static final net.sf.hibernate.dialect.Dialect GENERIC_DIALECT = new net.sf.hibernate.dialect.GenericDialect();

    private static Map TYPES_BY_NAME = new HashMap();
    private static Map TYPES_BY_TYPE = new HashMap();

    private static void registerType(String typeName, int type) {
        Integer objectType = new Integer(type);
        TYPES_BY_NAME.put(typeName, objectType);
        TYPES_BY_TYPE.put(objectType, typeName);
    }

    static {
        registerType("BIT", Types.BIT);
        registerType("INTEGER", Types.INTEGER);
        registerType("VARCHAR", Types.VARCHAR);
        registerType("CHAR", Types.CHAR);
        registerType("DATE", Types.DATE);
        registerType("NUMERIC", Types.NUMERIC);
        registerType("DECIMAL", Types.DECIMAL);
        registerType("TIMESTAMP", Types.TIMESTAMP);
    }

    public int getType(String dataTypeName) {
        if (!TYPES_BY_NAME.containsKey(dataTypeName)) throw new IllegalArgumentException("No registered type for name " + dataTypeName);
        return ((Integer) TYPES_BY_NAME.get(dataTypeName)).intValue();
    }

    public String getTypeName(int type) {
        Integer objectType = new Integer(type);
        if (!TYPES_BY_TYPE.containsKey(objectType)) throw new IllegalArgumentException("No registered type " + type);
        return (String) TYPES_BY_TYPE.get(objectType);
    }

    public String getTypeName(Column column) {
        return getTypeName(column.type());
    }

    public int getSize(Column column) {
        return column.size();
    }

    public Iterator getTables(DataSource dataSource, final String schemaName) throws SQLException {
        final List result = new LinkedList();
        MetaDataJdbcTemplate template = new MetaDataJdbcTemplate(dataSource) {
            protected ResultSet getResults(DatabaseMetaData metaData) throws SQLException {
                return metaData.getTables(null, schemaName, null, Table.Type.getSupportedNames());
            }
        };
        template.query(new RowCallbackHandler() {
            public void processRow(ResultSet tables) throws SQLException {
                result.add(new Table.Spec(tables.getString("TABLE_NAME"), tables.getString("TABLE_TYPE")));
            }
        });
        return result.iterator();
    }

    public Iterator getColumns(final DataSource dataSource, final String schemaName) throws SQLException {
        final List result = new LinkedList();
        MetaDataJdbcTemplate template = new MetaDataJdbcTemplate(dataSource) {
            protected ResultSet getResults(DatabaseMetaData metaData) throws SQLException {
                return metaData.getColumns(null, schemaName, null, null);
            }
        };
        template.query(new RowCallbackHandler() {
            public void processRow(ResultSet columns) throws SQLException {
                //retrieve values ahead of time, otherwise you get a stream closed error from Oracle
                String columnName = columns.getString("COLUMN_NAME");
                int dataType = columns.getInt("DATA_TYPE");
                String tableName = columns.getString("TABLE_NAME");
                int columnSize = columns.getInt("COLUMN_SIZE");
                int decimalDigits = columns.getInt("DECIMAL_DIGITS");
                boolean isNullable = columns.getBoolean("NULLABLE");
                String columnDef = columns.getString("COLUMN_DEF");
                result.add(new Column.Spec(tableName, new Column(columnName, dataType, columnSize, decimalDigits, isNullable, parseDefaultValue(columnDef, dataType))));
            }
        });
        return result.iterator();
    }

    public Iterator getPrimaryKeys(DataSource dataSource, final String schemaName) {
        final Map byTableName = new HashMap();
        MetaDataJdbcTemplate template = new MetaDataJdbcTemplate(dataSource) {
            protected ResultSet getResults(DatabaseMetaData metaData) throws SQLException {
                return metaData.getPrimaryKeys(null, schemaName, null);
            }
        };
        template.query(new RowCallbackHandler() {
            public void processRow(ResultSet primaryKeys) throws SQLException {
                String tableName = primaryKeys.getString("TABLE_NAME");
                if (!byTableName.containsKey(tableName)) {
                    byTableName.put(tableName, new PrimaryKey.Spec(tableName, primaryKeys.getString("PK_NAME")));
                }
                ((PrimaryKey.Spec) byTableName.get(tableName)).addColumnName(primaryKeys.getString("COLUMN_NAME"));
            }
        });
        return byTableName.values().iterator();
    }

    public Iterator getUniqueKeys(DataSource dataSource, final String schemaName) {
        final List result = new LinkedList();
        MetaDataJdbcTemplate template = new MetaDataJdbcTemplate(dataSource) {
            protected ResultSet getResults(DatabaseMetaData metaData) throws SQLException {
                return metaData.getIndexInfo(null, schemaName, null, true, true);
            }
        };
        template.query(new RowCallbackHandler() {
            public void processRow(ResultSet uniqueIndexes) throws SQLException {
                boolean isNonUnique = uniqueIndexes.getBoolean("NON_UNIQUE");
                if (!isNonUnique) {
                    String columnName = uniqueIndexes.getString("COLUMN_NAME");
                    String constraintName = uniqueIndexes.getString("INDEX_NAME");
                    String tableName = uniqueIndexes.getString("TABLE_NAME");

                    result.add(new UniqueConstraint.Spec(tableName, columnName, constraintName));
                }
            }
        });
        return result.iterator();
    }

    public Iterator getSequences(DataSource dataSource, String schemaName) throws SQLException {
        return new Iterator() {
            public boolean hasNext() {
                return false;
            }

            public Object next() {
                throw new NoSuchElementException("No elements at all");
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public boolean supportsUnique() {
        return GENERIC_DIALECT.supportsUnique();
    }

    public boolean supportsIdentityColumns() {
        return GENERIC_DIALECT.supportsIdentityColumns();
    }

    public boolean supportsViews() {
        return false;
    }

    public boolean supportsSequences() {
        return false;
    }

    public String getDriverClassName() {
        throw new UnsupportedOperationException(getClass().getName() + " does not support getDriverClassName()");
    }

    public String getShutdownSql() {
        return null;
    }

    public String getNextSequenceValueSql(String sequenceName) {
        throw new UnsupportedOperationException(getClass().getName() + " does not support getNextSequenceValueSql()");
    }

    /**
     * override internalGetIdentityColumnString. This method provides exception type translation
     */
    public final String getIdentityColumnString() {
        try {
            return internalGetIdentityColumnString();
        } catch (MappingException e) {
            throw new SchemaException(e);
        }
    }

    protected String internalGetIdentityColumnString() throws MappingException {
        return GENERIC_DIALECT.getIdentityColumnString();
    }

    public DefaultValue parseDefaultValue(String defaultValue, int type) {
        if (defaultValue == null) return null;
        return new DefaultValue(defaultValue, (type == Types.LONGVARCHAR || type == Types.VARCHAR));
    }

    public String formatDefaultValue(Column column) {
        return column.defaultValue().getValue();
    }
}
