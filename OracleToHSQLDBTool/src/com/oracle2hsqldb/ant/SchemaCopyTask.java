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

import org.apache.tools.ant.BuildException;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Ant task for copying a schema from one database to another.
 *
 * <p>Example use:</p>
 * <pre>
 * <p/>
 * &lt;target name="create.testing.db" depends="clean.testing.db, build" description="read Oracle schemas and write new HSQLDB files for testing"&gt;
 *   &lt;mkdir dir="${basedir}/hsqldb"/&gt;
 * <p/>
 *   &lt;typedef resource="edu/northwestern/bioinformatics/schemamule/antlib.xml" uri="http://bioinformatics.northwestern.edu/schemamule"/&gt;
 *   &lt;schemacopy xmlns="http://bioinformatics.northwestern.edu/schemamule"&gt;
 *     &lt;to uri="jdbc:hsqldb:${basedir}/hsqldb/testingdb" username="sa" password=""/&gt;
 *     &lt;from uri="${database}" name="copied_schema" username="copied_user_1" password="whatever"/&gt;
 *     &lt;from uri="${database}" username="copied_user_2" password="whatever"&gt;
 *       &lt;table name="T_NECESSARY_TABLE_1"/&gt;
 *       &lt;table name="T_NECESSARY_TABLE_2"/&gt;
 *       &lt;table name="T_NECESSARY_TABLE_3"/&gt;
 *     &lt;/from&gt;
 *   &lt;/schemacopy&gt;
 *   &lt;sql driver="org.hsqldb.jdbcDriver" url="jdbc:hsqldb:file:whatever" userid="sa" password="" classpathref="whatever" autocommit="true"&gt;
 *       GRANT ALL ON CLASS "com.oracle2hsqldb.HsqldbLibrary" TO PUBLIC;
 *       CREATE ALIAS TRUNC FOR "com.oracle2hsqldb.HsqldbLibrary.trunc";
 *       &lt;!--HACKTAG: create dummy DUAL table to enable Oracle-like syntax such as SELECT encrypt_string('whatever','whatever') FROM DUAL--&gt;
 *       CREATE TABLE DUAL (DUMMY VARCHAR(30));
 *       INSERT INTO DUAL VALUES ('DO NOT INSERT ANYTHING ELSE');
 *       &lt;!-- with hsqldb 1.7.2, you must explicitly shut down the server so that it will release its lock on the file
 *               (in case other classes in this process want to establish their own connections --&gt;
 *       SHUTDOWN;
 *   &lt;/sql&gt;
 * &lt;/target&gt;
 * </pre>
 *
 * @author Moses Hohman
 */
public class SchemaCopyTask extends SchemaTask {
    private static final int DEFAULT_BATCH_SIZE = 1;

    private int batchSize;
    private SchemaParams to;

    public SchemaCopyTask() {
        batchSize = DEFAULT_BATCH_SIZE;
    }

    public void execute() throws BuildException {
        validate();
        try {
            Connection connection = to.getConnection();
            StatementBatch statement = null;
            try {
            	connection.createStatement().execute("create table a (id integer)");
            	connection.commit();
            	
                statement = new StatementBatch(connection.createStatement(), batchSize);
                to.writeSchemas(readSchemas(), statement);
                cleanUp(statement);
                statement.flush();
            } finally {
                if (statement != null) statement.close();
                connection.close();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new BuildException(e);
        }
    }

    public void validate() throws BuildException {
        super.validate();
        if (to == null) throw new BuildException("to subelement is required");
        if (batchSize < 1) throw new BuildException("batchsize argument must be >= 1");
    }

    private void cleanUp(StatementBatch statement) throws SQLException {
        to.teardown(statement);
    }

    // bean methods

    public void addConfiguredTo(SchemaParams schema) {
        preprocessAndValidate(schema);
        if (to != null) throw new BuildException("only one to subelement is allowed");
        if (schema.getIncludedTables().size() != 0) throw new BuildException("to subelement does not support table subelements");
        to = schema;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

}