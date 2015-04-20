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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.oracle2hsqldb.Column;
import com.oracle2hsqldb.DefaultValue;
import com.oracle2hsqldb.PrimaryKey;
import com.oracle2hsqldb.Sequence;
import com.oracle2hsqldb.Table;
import com.oracle2hsqldb.UniqueConstraint;
import com.oracle2hsqldb.View;


/**
 * @author Moses Hohman
 */
public class Oracle9Dialect extends GenericDialect {
    protected final Logger log = Logger.getLogger(getClass());

    private static final Map<String, Integer> TYPES_BY_NAME = new HashMap<String, Integer>();
    private static final Map<Integer, String> TYPES_BY_TYPE = new HashMap<Integer, String>();

    static {
        registerType("TIMESTAMP(3)", "TIMESTAMP", Types.TIMESTAMP);
        registerType("TIMESTAMP(6)", "TIMESTAMP", Types.TIMESTAMP);
        registerType("NUMBER", Types.NUMERIC);
        registerType("INTEGER", Types.INTEGER);
        registerType("VARCHAR2", Types.VARCHAR);
        registerType("CHAR", Types.CHAR);
        registerType("CLOB", Types.CLOB);
        registerType("BLOB", Types.BLOB);
        registerType("FLOAT", Types.FLOAT);
        registerType("LONG", Types.BIGINT);
        

        // treat DATE asymmetrically since an Oracle DATE is actually
        // a TIMESTAMP(0) [non-conforming bastards] and is used as such
        // in schemas we can't change.
        TYPES_BY_NAME.put("DATE", new Integer(Types.TIMESTAMP));
        TYPES_BY_TYPE.put(new Integer(Types.DATE), "DATE");
    }

    private static void registerType(String typeName, int type) {
        Integer objectType = new Integer(type);
        TYPES_BY_NAME.put(typeName, objectType);
        TYPES_BY_TYPE.put(objectType, typeName);
    }

    private static void registerType(String typeName, String formatName, int type) {
        registerType(typeName, type);
        TYPES_BY_TYPE.put(new Integer(type), formatName); //corrected
    }

    public boolean supportsViews() {
        return true;
    }

    public boolean supportsSequences() {
        return true;
    }

    public String getDriverClassName() {
        return "oracle.jdbc.OracleDriver";
    }

    public String getNextSequenceValueSql(String sequenceName) {
        return new StringBuffer(sequenceName).append(".NEXTVAL").toString();
    }

    /**
     * performance improvement over GenericDialect's getTables()
     */
    @Override
    public List<Table.Spec> getTables(DataSource dataSource, String schemaName) throws SQLException {
    	if (isSchemaInfoAccessible()) {
            final List<Table.Spec> specs = new ArrayList<Table.Spec>();
	        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
	        jdbcTemplate.query("SELECT table_name FROM user_tables ", new RowCallbackHandler() {
				public void processRow(ResultSet result) throws SQLException {
					if (!"TOAD_PLAN_TABLE".equals(result.getString("TABLE_NAME"))) {
						specs.add(new Table.Spec(result.getString("TABLE_NAME"), Table.Type.TABLE
								.getJdbcName()));
					}
				}
			});
	        jdbcTemplate.query("SELECT view_name, text FROM user_views",
	                new RowCallbackHandler() {
	                    public void processRow(ResultSet result) throws SQLException {
	                        specs.add(new View.Spec(result.getString("VIEW_NAME"), Table.Type.VIEW.getJdbcName(), result.getString("TEXT")));
	                    }
	                });
	        return specs;
    	} else {
    		return super.getTables(dataSource, schemaName);
    	}
    }

    /**
     * performance improvement over GenericDialect's getColumns()
     */
    @Override
    public MultiValueMap<String, Column.Spec> getColumns(final DataSource dataSource, String schemaName, List<Table.Spec> tables) throws SQLException {
    	if (isSchemaInfoAccessible()) {
	        final MultiValueMap<String, Column.Spec> specs = new LinkedMultiValueMap<String, Column.Spec>();
	        new JdbcTemplate(dataSource).query("SELECT " +
	                "column_name, " +
	                "table_name, " +
	                "data_type, " +
	                "NVL(data_precision, data_length) AS column_size," +
	                "data_scale AS decimal_digits," +
	                "DECODE(nullable, 'Y', 1, 0) AS nullable," +
	                "data_default AS column_def " +
	                "FROM user_tab_columns",
	                new RowCallbackHandler() {
	                    public void processRow(ResultSet columns) throws SQLException {
					// retrieve values ahead of time, otherwise you get a stream
					// closed error from Oracle
					String columnName = columns.getString("COLUMN_NAME");
					if (log.isDebugEnabled())
						log.debug("Reading column " + columnName);
					String tableName = columns.getString("TABLE_NAME");
					try {
						int dataType = getType(columns.getString("DATA_TYPE"));
						
						int columnSize = columns.getInt("COLUMN_SIZE");
						int decimalDigits = columns.getInt("DECIMAL_DIGITS");
						boolean isNullable = columns.getBoolean("NULLABLE");
						String columnDef = columns.getString("COLUMN_DEF");
						specs.add(tableName, new Column.Spec(tableName, new Column(columnName, dataType, columnSize,
								decimalDigits, isNullable, parseDefaultValue(columnDef, dataType))));
					} catch (IllegalArgumentException e) {
						log.error("Problems with column " + columnName + " from table name  " + tableName);
						throw new SQLException("Problems with column " + columnName + " from table "
								+ tableName, e);
					}
				}
	                });
	        return specs;
    	} else {
    		return super.getColumns(dataSource, schemaName, tables);
    	}
    }

    /**
     * Superclass's implementation does not work. Seems that the Oracle 9i driver does not support DatabaseMetaData.getPrimaryKeys() well.
     */
    @Override
    public Map<String, PrimaryKey.Spec> getPrimaryKeys(DataSource dataSource, String schemaName, List<Table.Spec> tables) {
    	if (isSchemaInfoAccessible()) {
	        final Map<String, PrimaryKey.Spec> byTableName = new HashMap<String, PrimaryKey.Spec>();
	        new JdbcTemplate(dataSource).query("SELECT ucc.column_name, ucc.constraint_name, ucc.table_name " +
	                "FROM user_constraints uc INNER JOIN user_cons_columns ucc ON ucc.constraint_name=uc.constraint_name " +
	                "WHERE uc.constraint_type='P'",
	                new RowCallbackHandler() {
	                    public void processRow(ResultSet columns) throws SQLException {
	                        if (log.isDebugEnabled()) log.debug("Reading primary key:column " + columns.getString("CONSTRAINT_NAME") + ":" + columns.getString("COLUMN_NAME"));
	                        String tableName = columns.getString("TABLE_NAME");
	                        if (!byTableName.containsKey(tableName)) {
	                            byTableName.put(tableName, new PrimaryKey.Spec(tableName, columns.getString("CONSTRAINT_NAME")));
	                        }
	                        byTableName.get(tableName).addColumnName(columns.getString("COLUMN_NAME"));
	
	                    }
	                });
	        return byTableName;
    	} else {
    		return super.getPrimaryKeys(dataSource, schemaName, tables);
    	}
    }

    /**
     * Superclass's implementation does not work. Seems that the Oracle driver does not support DatabaseMetaData.getIndexInfo() well.
     */
    @Override
    public List<UniqueConstraint.Spec> getUniqueKeys(DataSource dataSource, String schemaName, List<Table.Spec> tables) {
    	if (isSchemaInfoAccessible()) {
	        final List<UniqueConstraint.Spec> specs = new ArrayList<UniqueConstraint.Spec>();
	        new JdbcTemplate(dataSource).query("SELECT ucc.column_name, ucc.constraint_name, ucc.table_name " +
	                "FROM user_constraints uc INNER JOIN user_cons_columns ucc ON ucc.constraint_name=uc.constraint_name " +
	                "WHERE uc.constraint_type='U'",
	                new RowCallbackHandler() {
	                    public void processRow(ResultSet columns) throws SQLException {
	                        String columnName = columns.getString("COLUMN_NAME");
	                        String tableName = columns.getString("TABLE_NAME");
	                        String constraintName = columns.getString("CONSTRAINT_NAME");
	                        if (log.isDebugEnabled()) log.debug("Reading unique constraint:column " + constraintName + ":" + columnName);
	                        specs.add(new UniqueConstraint.Spec(tableName, columnName, constraintName));
	                    }
	                });
	        return specs;
    	} else {
    		return super.getUniqueKeys(dataSource, schemaName, tables);
    	}
    }

    /**
     * Superclass returns nothing.
     */
    @Override
    public List<Sequence> getSequences(DataSource dataSource, String schemaName) throws SQLException {
    	if (isSchemaInfoAccessible()) {
	        final List<Sequence> seq = new ArrayList<Sequence>();
	        new JdbcTemplate(dataSource).query("SELECT sequence_name, last_number FROM user_sequences",
	                new RowCallbackHandler() {
	                    public void processRow(ResultSet rs) throws SQLException {
	                        String seqName = rs.getString("SEQUENCE_NAME");
	                        long seqValue = rs.getLong("LAST_NUMBER");
	                        if (log.isDebugEnabled()) log.debug("Reading sequence " + seqName + "; currval=" + seqValue);
	                        seq.add(new Sequence(seqName, new Long(seqValue)));
	                    }
	                });
	        return seq;
    	} else {
    		return super.getSequences(dataSource, schemaName);
    	}
    }

    private static final String SYSDATE_STRING = "SYSDATE";
    private static final String SYSTIMESTAMP_STRING = "SYSTIMESTAMP";
    private static final Set<String> NOW_STRINGS = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(new String[]{SYSDATE_STRING, SYSTIMESTAMP_STRING})));

    public DefaultValue parseDefaultValue(String defaultValue, int type) {
        if (defaultValue != null && NOW_STRINGS.contains(defaultValue.trim())) return DefaultValue.NOW;
        return super.parseDefaultValue(defaultValue, type);
    }

    public String formatDefaultValue(Column column) {
        DefaultValue value = column.defaultValue();
        if (DefaultValue.NOW.equals(value)) {
            if (column.type() == Types.DATE) {
                return SYSDATE_STRING;
            } else {
                return SYSTIMESTAMP_STRING;
            }
        }
        return super.formatDefaultValue(column);
    }

    public int getType(String dataTypeName) {
        if (!TYPES_BY_NAME.containsKey(dataTypeName)) throw new IllegalArgumentException("No registered Oracle type with name " + dataTypeName);
        return ((Integer) TYPES_BY_NAME.get(dataTypeName)).intValue();
    }

    public String getTypeName(int type) {
        final Integer objectType = new Integer(type);
        if (!TYPES_BY_TYPE.containsKey(objectType)) throw new IllegalArgumentException("No registered Oracle type " + type);
        return (String) TYPES_BY_TYPE.get(objectType);
    }
}