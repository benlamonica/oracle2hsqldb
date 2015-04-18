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

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.easymock.EasyMock;

import com.oracle2hsqldb.dialect.HSQLDialect;
import com.oracle2hsqldb.dialect.Oracle9Dialect;

/**
 * @author Moses Hohman
 */
public class SchemaParamsTest extends TestCase {
    private static final String HSQLDB_URI = "jdbc:hsqldb:/var/testhsqldb";
    private SchemaParams params = new SchemaParams();

    public void testSupportsHsqldb() throws URISyntaxException {
        params.setUri(HSQLDB_URI);
        assertEquals(HSQLDialect.class, params.getDialect().getClass());
    }

    public void testSupportsOracle() throws URISyntaxException {
        params.setUri("jdbc:oracle:thin@f.q.d.n:1521:sid");
        assertEquals(Oracle9Dialect.class, params.getDialect().getClass());
    }

    public void testThrowsBuildExceptionIfDatabaseNotSupported() throws URISyntaxException {
        try {
            params.setUri("jdbc:unsupported:blahblah");
            fail("Should have thrown BuildException");
        } catch (BuildException expected) {
            assertEquals("Dialect not supported: jdbc:unsupported", expected.getMessage());
        }
    }

    public void testSetUriHandlesNull() throws URISyntaxException {
        params.setUri(null);
        assertNull(params.getUri());
    }

    private void completeParams() throws URISyntaxException {
        params.setUri(HSQLDB_URI);
        params.setUsername("sa");
        params.setPassword("");
        params.setProject(new Project());
        params.setTask(new SchemaCopyTask());
    }

    public void testValidateThrowsBuildExceptionIfUriNotSet() throws URISyntaxException {
        completeParams();
        params.setUri(null);
        try {
            params.validate();
            fail("Should have thrown BuildException");
        } catch (BuildException expected) {
            assertEquals("uri argument is required", expected.getMessage());
        }
    }

    public void testValidateThrowsBuildExceptionIfUsernameNotSet() throws URISyntaxException {
        completeParams();
        params.setUsername(null);
        try {
            params.validate();
            fail("Should have thrown BuildException");
        } catch (BuildException expected) {
            assertEquals("username argument is required", expected.getMessage());
        }
    }

    public void testValidateThrowsBuildExceptionIfPasswordNotSet() throws URISyntaxException {
        completeParams();
        params.setPassword(null);
        try {
            params.validate();
            fail("Should have thrown BuildException");
        } catch (BuildException expected) {
            assertEquals("password argument is required", expected.getMessage());
        }
    }

    public void testValidateThrowsBuildExceptionIfProjectNotSet() throws URISyntaxException {
        completeParams();
        params.setProject(null);
        try {
            params.validate();
            fail("Should have thrown BuildException");
        } catch (BuildException expected) {
            assertEquals("project not set", expected.getMessage());
        }
    }

    public void testValidateThrowsBuildExceptionIfTaskNotSet() throws URISyntaxException {
        completeParams();
        params.setTask(null);
        try {
            params.validate();
            fail("Should have thrown BuildException");
        } catch (BuildException expected) {
            assertEquals("task not set", expected.getMessage());
        }
    }

    public void testDefaultCopyPrimaryKeysIsTrue() throws URISyntaxException {
        params.setUri(HSQLDB_URI);
        assertTrue(params.getConfiguration().supportsPrimaryKeys());
    }

    public void testDefaultCopyForeignKeysIsTrue() throws URISyntaxException {
        params.setUri(HSQLDB_URI);
        assertTrue(params.getConfiguration().supportsPrimaryKeys());
    }

    public void testIsAllTablesTrueIfNoTablesAdded() {
        assertTrue(params.isAllTables());
    }

    public void testAddConfiguredTableMakesIsAllTablesFalse() {
        params.addConfiguredTable(createTableParams("T_BLAH"));
        assertFalse(params.isAllTables());
    }

    public void testShouldWriteTableNameTrueForAnyTableIfNoTablesAdded() {
        assertTrue(params.shouldReadTable("ANY POSSIBLE TABLE NAME :)"));
    }

    public void testAddConfiguredTableMakesShouldWriteTableNameTrue() {
        String tableName = "T_BLAH";
        params.addConfiguredTable(createTableParams(tableName));
        assertTrue(params.shouldReadTable(tableName));
    }

    public void testShouldWriteTableNameFalseForTableNameOtherThanAddedTable() {
        String tableName = "T_BLAH";
        params.addConfiguredTable(createTableParams(tableName));
        assertFalse(params.shouldReadTable(tableName + "different"));
    }

    public void testNotAllTablesIfTableExcluded() {
        params.addConfiguredTable(createTableParams("UNLIKED", true));
        assertFalse(params.isAllTables());
    }

    public void testShouldNotReadExcludedTable() {
        params.addConfiguredTable(createTableParams("UNLIKED", true));
        assertFalse(params.shouldReadTable("UNLIKED"));
    }

    public void testShouldReadNonexcludedTableIfExcludesButNoIncludes() {
        params.addConfiguredTable(createTableParams("UNLIKED", true));
        assertTrue(params.shouldReadTable("INDIFFERENT"));
    }

    public void testShouldNotReadNonIncludedTableWhenExcludesAndIncludesPresent() {
        params.addConfiguredTable(createTableParams("LIKED", false));
        params.addConfiguredTable(createTableParams("UNLIKED", true));
        assertFalse(params.shouldReadTable("INDIFFERENT"));
    }

    public void testShouldNotReadExcludedTableWhenExcludesAndIncludesPresent() {
        params.addConfiguredTable(createTableParams("LIKED", false));
        params.addConfiguredTable(createTableParams("UNLIKED", true));
        assertFalse(params.shouldReadTable("UNLIKED"));
    }

    public void testShouldReadIncludedTableWhenExcludesAndIncludesPresent() {
        params.addConfiguredTable(createTableParams("LIKED", false));
        params.addConfiguredTable(createTableParams("UNLIKED", true));
        assertTrue(params.shouldReadTable("LIKED"));
    }

    public void testConfigurationRespectsPrimaryKeysAttribute() throws URISyntaxException {
        params.setUri(HSQLDB_URI);
        params.setPrimaryKeys(true);
        assertTrue("not true", params.getConfiguration().supportsPrimaryKeys());
        params.setPrimaryKeys(false);
        assertFalse("not false", params.getConfiguration().supportsPrimaryKeys());
    }

    public void testConfigurationRespectsForeignKeysAttribute() throws URISyntaxException {
        params.setUri(HSQLDB_URI);
        params.setForeignKeys(true);
        assertTrue("not true", params.getConfiguration().supportsForeignKeys());
        params.setForeignKeys(false);
        assertFalse("not false", params.getConfiguration().supportsForeignKeys());
    }

    public void testGetConnectionWorks() throws URISyntaxException, SQLException {
        params.setUri("jdbc:hsqldb:mem:whatever");
        params.setUsername("sa");
        params.setPassword("");
        Connection connection = null;
        Statement statement = null;
        try {
            connection = params.getConnection();
            statement = connection.createStatement();
            statement.executeUpdate("CREATE TABLE t_blah (id INTEGER)");
        } finally {
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        }
    }

    public void testTeardownCallsExecutesShutdownSql() throws SQLException, URISyntaxException {
        StatementBatch mock = EasyMock.createMock(StatementBatch.class);

        params.setUri("jdbc:hsqldb:mem:whatever");
        mock.executeUpdate(params.getDialect().getShutdownSql());
        EasyMock.replay(mock);

        params.teardown(mock);

        EasyMock.verify(mock);
    }

    public void testAddTableThrowsBuildExceptionIfTableIsInvalid() {
        try {
            params.addConfiguredTable(new TableParams());
            fail("Should have thrown BuildException");
        } catch (BuildException expected) {
            assertEquals("argument name is required", expected.getMessage());
        }
    }

    private TableParams createTableParams(String name) {
        return createTableParams(name, false);
    }

    private TableParams createTableParams(String name, boolean exclude) {
        TableParams result = new TableParams();
        result.setName(name);
        result.setExclude(exclude);
        return result;
    }
}
