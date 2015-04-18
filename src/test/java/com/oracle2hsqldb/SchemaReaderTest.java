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

import junit.framework.TestCase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;

import com.oracle2hsqldb.Column;
import com.oracle2hsqldb.PrimaryKey;
import com.oracle2hsqldb.Schema;
import com.oracle2hsqldb.SchemaReader;
import com.oracle2hsqldb.Table;
import com.oracle2hsqldb.UniqueConstraint;

/**
 * @author Moses Hohman
 */
public class SchemaReaderTest extends TestCase {
    private static final String BOOK_TABLE_NAME = "T_BOOKS";
    private static final String LIBRARY_TABLE_NAME = "T_LIBRARIES";

    static {
        try {
            Class.forName("org.hsqldb.jdbcDriver");
        } catch (Exception e) {
            System.err.println("ERROR: failed to load HSQLDB JDBC driver");
            e.printStackTrace(System.err);
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:hsqldb:.", "sa", "");
    }

    private Connection conn;
    private Schema schema;

    protected void setUp() throws Exception {
        conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE " + LIBRARY_TABLE_NAME + " (" +
                "id INTEGER IDENTITY PRIMARY KEY," +
                "name VARCHAR(30)," +
                "CONSTRAINT uk_library_name UNIQUE(name)" +
            ")");
        stmt.execute("CREATE TABLE " + BOOK_TABLE_NAME + " (" +
                "id INTEGER IDENTITY PRIMARY KEY, " +
                "title VARCHAR(50) DEFAULT 'unknown', " +
                "library_id INTEGER NOT NULL," +
                "cost DECIMAL(10,2)," +
                "CONSTRAINT fk_book_library FOREIGN KEY (library_id) REFERENCES " + LIBRARY_TABLE_NAME +
            ")");
        stmt.execute("CREATE TABLE t_folks (" +
                "id INTEGER IDENTITY PRIMARY KEY, " +
                "first_name VARCHAR(50), " +
                "last_name VARCHAR(50)," +
                "CONSTRAINT uk_folks_names UNIQUE(first_name, last_name)" +
            ")");
        stmt.execute("CREATE TABLE t_personnel (" +
                "id INTEGER IDENTITY PRIMARY KEY, " +
                "username VARCHAR(50), " +
                "CONSTRAINT uk_personel_un_id UNIQUE(id, username)" +
            ")");
        stmt.execute("INSERT INTO " + LIBRARY_TABLE_NAME + " VALUES (NULL, 'Chicago Public Library')");
        stmt.execute("INSERT INTO " + BOOK_TABLE_NAME + " VALUES (NULL, 'Hee-Haw Chronicles', 0, 5.25)");
        stmt.execute(
            "CREATE VIEW v_folks_with_s_names " +
            "AS SELECT f.id as id, f.first_name as first_name, f.last_name as last_name " +
            "FROM t_folks f " +
            "WHERE f.last_name LIKE 'S%'"
        );
        stmt.close();

        schema = new SchemaReader(conn).read(null);
    }

    protected void tearDown() throws Exception {
        Statement stmt = conn.createStatement();
        stmt.execute("DROP VIEW v_folks_with_s_names");
        stmt.execute("DROP TABLE t_personnel");
        stmt.execute("DROP TABLE t_folks");
        stmt.execute("DROP TABLE " + BOOK_TABLE_NAME);
        stmt.execute("DROP TABLE " + LIBRARY_TABLE_NAME);
        conn.close();
    }


    public void testTableNames() throws SQLException {
        assertEquals("number of tables wrong", 5, schema.tables().size());
        assertEquals(BOOK_TABLE_NAME, ((Table) schema.tables().get(0)).name());
        assertEquals("T_FOLKS", ((Table) schema.tables().get(1)).name());
        assertEquals(LIBRARY_TABLE_NAME, ((Table) schema.tables().get(2)).name());
        assertEquals("T_PERSONNEL", ((Table) schema.tables().get(3)).name());
        assertEquals("V_FOLKS_WITH_S_NAMES", ((Table) schema.tables().get(4)).name());
    }

    public void testTableTypes() throws SQLException {
        assertEquals(Table.Type.TABLE, schema.findTable(BOOK_TABLE_NAME).type());
        assertEquals(Table.Type.TABLE, schema.findTable("T_FOLKS").type());
        assertEquals(Table.Type.TABLE, schema.findTable(LIBRARY_TABLE_NAME).type());
        assertEquals(Table.Type.TABLE, schema.findTable("T_PERSONNEL").type());
        assertEquals(Table.Type.VIEW, schema.findTable("V_FOLKS_WITH_S_NAMES").type());
        assertEquals("number of tables wrong", 5, schema.tables().size());
    }

    public void testColumnNames() throws SQLException {
        List libraryColumns = schema.findTable(LIBRARY_TABLE_NAME).columns();
        assertEquals("num columns on t_libraries wrong", 2, libraryColumns.size());
        Column column1 = (Column) libraryColumns.get(0);
        Column column2 = (Column) libraryColumns.get(1);
        assertEquals("column 1 name wrong", "ID", column1.name());
        assertEquals("column 2 name wrong", "NAME", column2.name());
    }

    public void testColumnTypes() throws SQLException {
        List libraryColumns = schema.findTable(LIBRARY_TABLE_NAME).columns();
        assertEquals("num columns on t_libraries wrong", 2, libraryColumns.size());
        Column column1 = (Column) libraryColumns.get(0);
        Column column2 = (Column) libraryColumns.get(1);
        assertEquals("column 1 type wrong", Types.INTEGER, column1.type());
        assertEquals("column 2 type wrong", Types.VARCHAR, column2.type());
    }

    public void testNullable() throws SQLException {
        List libraryColumns = schema.findTable(LIBRARY_TABLE_NAME).columns();
        assertEquals("num columns on t_libraries wrong", 2, libraryColumns.size());
        Column column1 = (Column) libraryColumns.get(0);
        Column column2 = (Column) libraryColumns.get(1);
        assertEquals("column 1 isNullable wrong", false, column1.isNullable());
        assertEquals("column 2 isNullable wrong", true, column2.isNullable());
    }

    public void testSize() throws SQLException {
        List libraryColumns = schema.findTable(LIBRARY_TABLE_NAME).columns();
        assertEquals("num columns on t_libraries wrong", 2, libraryColumns.size());
        Column column1 = (Column) libraryColumns.get(0);
        Column column2 = (Column) libraryColumns.get(1);
        assertEquals("column 1 size wrong", 32, column1.size());
        assertEquals("column 2 size wrong", 30, column2.size());
    }

    public void testPrecision() {
        Table t_books = schema.findTable(BOOK_TABLE_NAME);
        Column cost = t_books.findColumn("COST");
        assertEquals(2, cost.precision());
    }

    public void testColumnOwner() throws SQLException {
        Table t_libraries = schema.findTable(LIBRARY_TABLE_NAME);
        List libraryColumns = t_libraries.columns();
        Column column1 = (Column) libraryColumns.get(0);
        assertEquals(t_libraries, column1.owner());
    }

    public void testForeignKey() throws SQLException {
        Table t_libraries = schema.findTable(LIBRARY_TABLE_NAME);
        Table t_books = schema.findTable(BOOK_TABLE_NAME);
        Column foreignKey = (Column) t_books.columns().get(2);
        Column primaryKey = (Column) t_libraries.columns().get(0);
        //sanity check
        assertEquals("foreign key wrong", "LIBRARY_ID", foreignKey.name());
        assertEquals("primary key wrong", "ID", primaryKey.name());

        //now the real util
        assertEquals("reference refersTo wrong", primaryKey, foreignKey.reference().refersTo());
        assertEquals("reference name wrong", "FK_BOOK_LIBRARY", foreignKey.reference().name());
        assertNull("primary key not a reference", primaryKey.reference());
    }

    public void testPrimaryKey() {
        Table t_libraries = schema.findTable(LIBRARY_TABLE_NAME);
        PrimaryKey pk = t_libraries.primaryKey();
        assertEquals("number of columns in pk wrong", 1, pk.columns().size());
        assertEquals("pk name wrong", "SYS_PK_10092", pk.name());
        assertEquals("pk column name wrong", "ID", ((Column) pk.columns().get(0)).name());
    }

    public void testUnique() {
        Table t_libraries = schema.findTable(LIBRARY_TABLE_NAME);
        Column nameColumn = t_libraries.findColumn("NAME");
        assertTrue("is not unique", nameColumn.isUnique());
        assertTrue("is not member", nameColumn.isUniqueKeyMember());
    }

    public void testNonUnique() {
        Table t_books = schema.findTable(BOOK_TABLE_NAME);
        Column cost = t_books.findColumn("COST");
        assertFalse("is unique", cost.isUnique());
        assertFalse("is unique key member", cost.isUniqueKeyMember());
    }

    public void testUniqueKeyWithTwoColumns() {
        Table t_folks = schema.findTable("T_FOLKS");

        UniqueConstraint tableUniqueKey = t_folks.findConstraint("SYS_IDX_UK_FOLKS_NAMES"); //hsqldb names the indexes differently from the constraints
        assertNotNull("table unique key null", tableUniqueKey);

        Column firstName = t_folks.findColumn("FIRST_NAME");
        Column lastName = t_folks.findColumn("LAST_NAME");
        assertTrue("firstName not unique key member", firstName.isUniqueKeyMember());
        assertTrue("lastName not unique key member", lastName.isUniqueKeyMember());
        assertFalse("firstName is unique", firstName.isUnique());
        assertFalse("lastName is unique", lastName.isUnique());

        List uniqueKeys = firstName.uniqueConstraints();
        assertEquals("more than 1 unique key", 1, uniqueKeys.size());
        UniqueConstraint uniqueKey = (UniqueConstraint) uniqueKeys.get(0);

        assertEquals("unique keys not the same between table and column", tableUniqueKey, uniqueKey);

        assertEquals("wrong number of columns", 2, uniqueKey.columns().size());
        assertEquals("wrong first column", firstName, uniqueKey.columns().get(0));
        assertEquals("wrong last column", lastName, uniqueKey.columns().get(1));
    }

    public void testDoesNotCreateUniqueKeyForPrimaryKey() {
        Table t_books = schema.findTable(BOOK_TABLE_NAME);
        assertEquals(0, t_books.constraints().size());
    }

    public void testAllowsPrimaryKeyToParticipateInMulticolumnUniqueKey() {
        Table t_personnel = schema.findTable("T_PERSONNEL");
        assertEquals("no constraints", 1, t_personnel.constraints().size());
        UniqueConstraint constraint = (UniqueConstraint) t_personnel.constraints().get(0);
        assertEquals(2, constraint.columns().size());
        assertEquals("ID", ((Column) constraint.columns().get(0)).name());
        assertEquals("USERNAME", ((Column) constraint.columns().get(1)).name());
    }

    public void testDefault() {
        Table t_books = schema.findTable(BOOK_TABLE_NAME);
        Column titleColumn = t_books.findColumn("TITLE");
        assertTrue("default value not string", titleColumn.defaultValue().isString());
        assertEquals("'unknown'", titleColumn.defaultValue().getValue());
    }
}