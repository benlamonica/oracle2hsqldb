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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.hibernate.MappingException;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.oracle2hsqldb.Column;
import com.oracle2hsqldb.DefaultValue;
import com.oracle2hsqldb.Index;
import com.oracle2hsqldb.PrimaryKey;
import com.oracle2hsqldb.SchemaException;
import com.oracle2hsqldb.Sequence;
import com.oracle2hsqldb.Table;
import com.oracle2hsqldb.TableFilter;
import com.oracle2hsqldb.spring.MetaDataJdbcTemplate;

/**
 * @author Moses Hohman
 */
public class GenericDialect implements Dialect {
	private static class HibernateGenericDialect extends org.hibernate.dialect.Dialect {
		@Override
		public String getIdentityColumnString() {
			return super.getIdentityColumnString();
		}
	}
    private static final HibernateGenericDialect GENERIC_DIALECT = new HibernateGenericDialect();

    private static Map<String, Integer> TYPES_BY_NAME = new HashMap<String, Integer>();
    private static Map<Integer, String> TYPES_BY_TYPE = new HashMap<Integer, String>();

    private static void registerType(String typeName, int type) {
        TYPES_BY_NAME.put(typeName, type);
        TYPES_BY_TYPE.put(type, typeName);
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

    private boolean schemaInfoAccessible = true;
    
    @Override
	public boolean isSchemaInfoAccessible() {
		return schemaInfoAccessible;
	}

	@Override
	public void setSchemaInfoAccessible(boolean schemaInfoAccessible) {
		this.schemaInfoAccessible = schemaInfoAccessible;
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

    @Override
    public List<Table.Spec> getTables(DataSource dataSource, final String schemaName, final TableFilter filter) throws SQLException {
        final List<Table.Spec> result = new ArrayList<Table.Spec>();
        MetaDataJdbcTemplate template = new MetaDataJdbcTemplate(dataSource) {
            protected ResultSet getResults(DatabaseMetaData metaData) throws SQLException {
                return metaData.getTables(null, schemaName, null, Table.Type.getSupportedNames());
            }
        };
        template.query(new RowCallbackHandler() {
            public void processRow(ResultSet tables) throws SQLException {
            	Table.Spec spec = new Table.Spec(tables.getString("TABLE_NAME"), tables.getString("TABLE_TYPE"));
            	if (filter == null || (filter != null && filter.accept(spec.getTable()))) {
                    result.add(spec);
            	}
            }
        });
        return result;
    }

    @Override
    public MultiValueMap<String, Column.Spec> getColumns(final DataSource dataSource, final String schemaName, List<Table.Spec> tables) throws SQLException {
        final MultiValueMap<String, Column.Spec> result = new LinkedMultiValueMap<String, Column.Spec>();
        for (final Table.Spec table : tables) {
	        MetaDataJdbcTemplate template = new MetaDataJdbcTemplate(dataSource) {
	            protected ResultSet getResults(DatabaseMetaData metaData) throws SQLException {
	                return metaData.getColumns(null, schemaName, table.getTableName(), null);
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
	                result.add(tableName, new Column.Spec(tableName, new Column(columnName, dataType, columnSize, decimalDigits, isNullable, parseDefaultValue(columnDef, dataType))));
	            }
	        });
        }
        return result;
    }

    @Override
    public Map<String, PrimaryKey.Spec> getPrimaryKeys(DataSource dataSource, final String schemaName, List<Table.Spec> tables) {
        final Map<String, PrimaryKey.Spec> byTableName = new HashMap<String, PrimaryKey.Spec>();
        for (final Table.Spec table : tables) {
	        MetaDataJdbcTemplate template = new MetaDataJdbcTemplate(dataSource) {
	            protected ResultSet getResults(DatabaseMetaData metaData) throws SQLException {
	                return metaData.getPrimaryKeys(null, schemaName, table.getTableName());
	            }
	        };
	        template.query(new RowCallbackHandler() {
	            public void processRow(ResultSet primaryKeys) throws SQLException {
	                String tableName = primaryKeys.getString("TABLE_NAME");
	                if (!byTableName.containsKey(tableName)) {
	                    byTableName.put(tableName, new PrimaryKey.Spec(tableName, primaryKeys.getString("PK_NAME")));
	                }
	                byTableName.get(tableName).addColumnName(primaryKeys.getString("COLUMN_NAME"));
	            }
	        });
		}
        return byTableName;
    }

    @Override
    public List<Index.Spec> getIndicies(DataSource dataSource, final String schemaName, List<Table.Spec> tables) {
        final List<Index.Spec> result = new ArrayList<Index.Spec>();
        
        final RowCallbackHandler INDEX_CALLBACK_HANDLER = new RowCallbackHandler() {
            public void processRow(ResultSet uniqueIndexes) throws SQLException {
                boolean isNonUnique = uniqueIndexes.getBoolean("NON_UNIQUE");
                String columnName = uniqueIndexes.getString("COLUMN_NAME");
                String constraintName = uniqueIndexes.getString("INDEX_NAME");
                String tableName = uniqueIndexes.getString("TABLE_NAME");
                result.add(new Index.Spec(tableName, columnName, constraintName, !isNonUnique));
            }
        };

        for (final Table.Spec table : tables) {
        	// first get the unique indexes
	        MetaDataJdbcTemplate template = new MetaDataJdbcTemplate(dataSource) {
	            protected ResultSet getResults(DatabaseMetaData metaData) throws SQLException {
	                return metaData.getIndexInfo(null, schemaName, table.getTableName(), false, true);
	            }
	        };
	        template.query(INDEX_CALLBACK_HANDLER);
	    }
        return result;
    }

    public List<Sequence> getSequences(DataSource dataSource, String schemaName) throws SQLException {
    	return Collections.emptyList();
    }

    public boolean supportsUnique() {
        return true;
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
