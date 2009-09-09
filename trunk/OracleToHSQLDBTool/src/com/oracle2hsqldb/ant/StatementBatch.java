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

import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Moses Hohman
 */
public class StatementBatch {
    private Statement statement;
    private final int batchSize;
    private int batchCount = 0;

    public StatementBatch(Statement statement, int batchSize) {
        this.statement = statement;
        this.batchSize = batchSize;
    }

    public void executeUpdate(String sql) throws SQLException {
        if (batchSize < 2) {
            statement.executeUpdate(sql);
        } else {
            addToBatch(sql);
        }
    }

    private void addToBatch(String sql) throws SQLException {
    	System.out.println("Adding sql: " + sql);
        statement.addBatch(sql);
        batchCount++;
        if (batchCount >= batchSize) {
            statement.executeBatch();
            batchCount = 0;
        }
    }

    public void flush() throws SQLException {
        if (batchCount != 0) {
            statement.executeBatch();
            batchCount = 0;
        }
    }

    public void close() throws SQLException {
        statement.close();
    }
}
