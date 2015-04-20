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

package com.oracle2hsqldb.ant;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;

import com.oracle2hsqldb.Configuration;
import com.oracle2hsqldb.Schema;
import com.oracle2hsqldb.SchemaReader;
import com.oracle2hsqldb.SchemaWriter;
import com.oracle2hsqldb.Sequence;
import com.oracle2hsqldb.Table;
import com.oracle2hsqldb.TableFilter;
import com.oracle2hsqldb.dialect.Dialect;
import com.oracle2hsqldb.dialect.HSQLDialect;
import com.oracle2hsqldb.dialect.Oracle9Dialect;

/**
 * @author Moses Hohman
 */
public class SchemaParams implements Validatable, TableFilter {
    private Project project;
    private Task parent;
    private URI uri;
    private Dialect dialect;
    private String username;
    private String schema;
    private boolean schemaInfoAccessible = true;
    private boolean copyPrimaryKeys = true;
    private boolean copyForeignKeys = false;
    private boolean copySequences = false;
    private boolean viewsAsTables = false;
    private transient String password;
    private Set<String> includedTables = new HashSet<String>();
    private Set<String> excludedTables = new HashSet<String>();

    private static Map<String, Dialect> dialects = new HashMap<String, Dialect>();

    static {
        register("jdbc:oracle", new Oracle9Dialect());
        register("jdbc:hsqldb", new HSQLDialect());
    }

    private static void register(String urlPrefix, Dialect dialect) {
        dialects.put(urlPrefix, dialect);
    }

    // business methods

    public void validate() throws BuildException {
        if (project == null) throw new BuildException("project not set");
        if (parent == null) throw new BuildException("task not set");
        if (uri == null) throw new BuildException("uri argument is required");
        if (username == null) throw new BuildException("username argument is required");
        if (password == null) throw new BuildException("password argument is required");
    }

    public Schema readSchema() throws SQLException {
        log("reading schema (" + getSchema() + ") from database " + getUri() + " for user " + getUsername());
        Connection connection = getConnection();
        try {
            SchemaReader reader = new SchemaReader(getConfiguration(), connection);
            return reader.read(getSchema(), this);
        } finally {
            connection.close();
        }
    }

    public void writeSchemas(Schema[] schemas, StatementBatch statement) throws SQLException {
        SchemaWriter writer = new SchemaWriter(getConfiguration());
        writer.setViewsAsTables(viewsAsTables);
        for (int i = 0; i < schemas.length; i++) {
            log("writing schema " + schemas[i].name());
            for (Iterator<Table> tables = schemas[i].tables().iterator(); tables.hasNext();) {
                Table table = tables.next();
                log("writing table: " + table.name() + "\n" + writer.write(table), Project.MSG_VERBOSE);
                statement.executeUpdate(writer.write(table));
            }
            if (copySequences) {
                log("WRITING SEQUENCES", Project.MSG_VERBOSE);
                for (Iterator<Sequence> sequences = schemas[i].sequences().iterator(); sequences.hasNext();) {
                    Sequence sequence = sequences.next();
                    log("writing sequence: " + sequence.name() + "\n" + writer.write(sequence), Project.MSG_VERBOSE);
                    statement.executeUpdate(writer.write(sequence));
                }
            } else {
                log("NOT WRITING SEQUENCES", Project.MSG_VERBOSE);
            }
        }
    }

    public void teardown(StatementBatch statement) throws SQLException {
        if (getDialect().getShutdownSql() != null) {
            statement.executeUpdate(getDialect().getShutdownSql());
        }
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(getUri().toString(), username, password);
    }

    public boolean isAllTables() {
        return includedTables.isEmpty() && excludedTables.isEmpty();
    }

    public boolean accept(Table table) {
        return shouldReadTable(table.name());
    }

    public boolean shouldReadTable(String tableName) {
        if (isAllTables()) {
            // all
            return true;
        } else if (includedTables.contains(tableName)) {
            // explicitly included
            return true;
        } else if (excludedTables.contains(tableName)) {
            // explicitly excluded
            return false;
        } else {
            // implicitly included if none are explicitly included
            return includedTables.isEmpty();
        }
    }

    public Dialect getDialect() {
        return dialect;
    }

    Configuration getConfiguration() {
        return new Configuration(copyPrimaryKeys, copyForeignKeys, copySequences, dialect);
    }

    private void loadDialect() {
		try {
			dialect = dialects.get(getJdbcPrefix()).getClass().newInstance();
		} catch (Exception e) {
			// unable to instantiate our own instance of the dialect, so I guess use the shared one.
			dialect = dialects.get(getJdbcPrefix());
		}
        if (dialect == null) throw new BuildException("Dialect not supported: " + getJdbcPrefix());
        dialect.setSchemaInfoAccessible(schemaInfoAccessible);
        ensureDriverLoaded(dialect);
    }

    private String getJdbcPrefix() {
        return new StringBuffer(uri.getScheme()).append(":")
                .append(uri.getSchemeSpecificPart().substring(0, uri.getSchemeSpecificPart().indexOf(":"))).toString();
    }

    private static void ensureDriverLoaded(Dialect dialect) {
        try {
            Class.forName(dialect.getDriverClassName());
        } catch (ClassNotFoundException e) {
            throw new BuildException(e);
        }
    }

    private void log(String message) {
        log(message, Project.MSG_INFO);
    }

    private void log(String message, int msgLevel) {
        parent.log(message, msgLevel);
    }

    // bean methods

    public void setTask(Task task) {
        this.parent = task;
    }

    public void setProject(Project project) {
        this.project = project;
    }

    public void addConfiguredTable(TableParams table) {
        table.validate();
        if (table.isExclude()) {
            excludedTables.add(table.getName());
        } else {
            includedTables.add(table.getName());
        }
    }

    public URI getUri() {
        return uri;
    }

    public void setUri(String uri) throws URISyntaxException {
        this.uri = (uri == null ? null : new URI(uri));
        if (this.uri != null) loadDialect();
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getUsername() {
        return username;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getSchema() {
        return schema;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setPrimaryKeys(boolean copyPrimaryKeys) {
        this.copyPrimaryKeys = copyPrimaryKeys;
    }

    public void setForeignKeys(boolean copyForeignKeys) {
        this.copyForeignKeys = copyForeignKeys;
    }

    public void setViewsAsTables(boolean viewsAsTables) {
        this.viewsAsTables = viewsAsTables;
    }

    public void setCopySequences(boolean copySequences) {
        this.copySequences = copySequences;
    }

    public boolean isSchemaInfoAccessible() {
		return schemaInfoAccessible;
	}

	public void setSchemaInfoAccessible(boolean schemaInfoAccessible) {
		this.schemaInfoAccessible = schemaInfoAccessible;
	}

	public Set<String> getIncludedTables() {
        return includedTables;
    }

    public Set<String> getExcludedTables() {
        return excludedTables;
    }
}