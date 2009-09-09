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

import org.apache.log4j.Logger;

import com.oracle2hsqldb.Configuration;
import com.oracle2hsqldb.Schema;
import com.oracle2hsqldb.SchemaReader;
import com.oracle2hsqldb.SchemaWriter;
import com.oracle2hsqldb.Table;
import com.oracle2hsqldb.dialect.HSQLDialect;
import com.oracle2hsqldb.dialect.Oracle9Dialect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

/**
 * @author Moses Hohman
 */
public class OracleTest {
    private static final Logger LOG = Logger.getLogger(OracleTest.class);

    static {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (ClassNotFoundException e) {
            LOG.error("ERROR: failed to load Oracle JDBC driver", e);
        }

        try {
            Class.forName("org.hsqldb.jdbcDriver");
        } catch (Exception e) {
            LOG.error("ERROR: failed to load HSQLDB JDBC driver", e);
        }

    }

    public static void main(String[] args) throws SQLException {
        Connection oracle = DriverManager.getConnection("jdbc:oracle:thin:@129.105.22.88:1521:swifty", "cfg_animal", "waksbg");
        Schema schema = new SchemaReader(new Configuration(true, false, true, new Oracle9Dialect()), oracle).read("CFG_DIST");
        oracle.close();
        Iterator tables = schema.tables().iterator();
        SchemaWriter writer = new SchemaWriter(new Configuration(true, false, true, new HSQLDialect()));

        Connection hsqldb = DriverManager.getConnection("jdbc:hsqldb:.", "sa", "");

        Statement stmt = hsqldb.createStatement();

        while (tables.hasNext()) {
            Table table = (Table) tables.next();
            LOG.info("Creating table " + table.name());
            LOG.debug("Table type: " + table.type());
            LOG.debug("Table class: " + table.getClass().getName());
            LOG.debug("Primary key is " + table.primaryKey());
            if (table.primaryKey()!=null) {
                LOG.debug("Primary key column is " + table.primaryKey().columns());
            }
            LOG.debug("\n" + writer.write(table));
            stmt.execute(writer.write(table));
        }

        stmt.close();
        hsqldb.close();
    }
}