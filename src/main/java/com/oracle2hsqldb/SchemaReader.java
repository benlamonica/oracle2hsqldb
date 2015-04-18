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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.util.MultiValueMap;

/**
 * @author Moses Hohman
 */
public class SchemaReader {
    protected Logger log = Logger.getLogger(getClass());

    private Configuration config;
    private DataSource dataSource;

    public SchemaReader(Connection connection) {
        this(Configuration.DEFAULT_CONFIG, connection);
    }

    public SchemaReader(Configuration config, Connection connection) {
        this.config = config;
        this.dataSource = new SingleConnectionDataSource(connection, true);
    }

    public Configuration configuration() {
        return config;
    }

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public Schema read(String schemaName) throws SQLException {
        return read(schemaName, AllTablesFilter.INSTANCE);
    }

    public Schema read(String schemaName, TableFilter filter) throws SQLException {
        Schema schema = new Schema(schemaName);

        log.info("Reading tables ...");
        List<Table.Spec> tables = configuration().dialect().getTables(dataSource, schemaName);
        for (Table.Spec spec : tables) {
            if (filter.accept(spec.getTable())) {
                log.debug("Accepted table " + spec.getTableName());
                schema.addTable(spec.getTable());
            } else {
                log.debug("Skipped table " + spec.getTableName());
            }
        }

        log.info("Reading columns ...");
        MultiValueMap<String, Column.Spec> columns = configuration().dialect().getColumns(dataSource, schemaName, tables);
        for (Table.Spec table : tables) {
        	for (Column.Spec column : columns.get(table.getTableName())) {
        		Table t = schema.findTable(table.getTableName());
        		if (t != null) {
        			t.addColumn(column.getColumn());
        		}
        	}
		}

        if (configuration().supportsPrimaryKeys()) readPrimaryKeys(schema, tables);
        if (configuration().supportsForeignKeys()) readForeignKeys(schema);
        if (configuration().supportsUniqueKeys()) readUniqueKeys(schema, tables);

        log.info("Supports sequences? " + configuration().supportsSequences());
        if (configuration().supportsSequences()) readSequences(schemaName, schema);

        log.info("Schema read!");
        return schema;
    }

    private void readSequences(String schemaName, Schema schema) throws SQLException {
        log.info("Reading sequences ...");

        List<Sequence> seq = configuration().dialect().getSequences(dataSource, schemaName);
        for (Sequence sequence : seq) {
        	schema.addSequence(sequence);
		}
    }

    private void readPrimaryKeys(Schema schema, List<Table.Spec> tables) {
        log.info("Reading primary keys...");

        Map<String, PrimaryKey.Spec> keys = configuration().dialect().getPrimaryKeys(dataSource, schema.name(), tables);
        for (Table.Spec table : tables) {
        	PrimaryKey.Spec pk = keys.get(table.getTableName());
        	if (pk != null) {
        		Table t = schema.findTable(table.getTableName());
        		if (t!=null) {
        			pk.addPrimaryKey(t);
        		}
        	}
		}
    }

    private void readUniqueKeys(Schema schema, List<Table.Spec> tables) {
        log.info("Reading unique keys...");

        List<UniqueConstraint.Spec> keys = configuration().dialect().getUniqueKeys(dataSource, schema.name(), tables);
        for (UniqueConstraint.Spec spec : keys) {
            Table table = schema.findTable(spec.getTableName());
            if (table != null && spec.getColumnName() != null) {
                Column indexed = table.findColumn(spec.getColumnName());
                UniqueConstraint uniqueConstraint = table.findConstraint(spec.getConstraintName());
                if (uniqueConstraint == null) {
                    uniqueConstraint = new UniqueConstraint(spec.getConstraintName());
                }
                indexed.constrainBy(uniqueConstraint);
            }
        }

        removeUniqueKeysOnlyContainingPrimaryKey(schema);
    }

    private void removeUniqueKeysOnlyContainingPrimaryKey(Schema schema) {
        Iterator tables = schema.tables().iterator();

        while (tables.hasNext()) {
            Table table = (Table) tables.next();
            Iterator constraints = table.constraints().iterator();
            while (constraints.hasNext()) {
                UniqueConstraint constraint = (UniqueConstraint) constraints.next();
                if (constraint.columns().size() == 1 && ((Column) constraint.columns().get(0)).isPrimaryKeyMember()) {
                    table.removeConstraint(constraint);
                }
            }
        }
    }

    private void readForeignKeys(Schema schema) throws SQLException {
        log.info("Reading foreign keys...");

        Iterator foreignTables = schema.tables().iterator();

        while (foreignTables.hasNext()) {
            Table foreignTable = (Table) foreignTables.next();
            ResultSet importedKeys = getConnection().getMetaData().getImportedKeys(null, null, foreignTable.name());

            while (importedKeys.next()) {
                Table primaryTable = schema.findTable(importedKeys.getString("PKTABLE_NAME"));
                boolean primaryTableExistsInThisSchema = (primaryTable != null);
                if (primaryTableExistsInThisSchema) {
                    Column primaryColumn = primaryTable.findColumn(importedKeys.getString("PKCOLUMN_NAME"));
                    Column foreignColumn = foreignTable.findColumn(importedKeys.getString("FKCOLUMN_NAME"));
                    Reference ref = new Reference(importedKeys.getString("FK_NAME"), primaryColumn);
                    foreignColumn.reference(ref);
                }
            }

            importedKeys.close();
        }
    }
}
