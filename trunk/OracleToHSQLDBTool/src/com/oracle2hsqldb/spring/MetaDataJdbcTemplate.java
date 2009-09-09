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

package com.oracle2hsqldb.spring;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.dao.UncategorizedDataAccessException;
import org.springframework.dao.CleanupFailureDataAccessException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;

/**
 * @author Moses Hohman
 */
public abstract class MetaDataJdbcTemplate {
    protected Logger log = Logger.getLogger(getClass());

    private DataSource dataSource;

    public MetaDataJdbcTemplate(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void query(RowCallbackHandler rowHandler) {
        Connection connection = getConnection();

        ResultSet results = null;
        try {
            results = getResults(connection.getMetaData());
            while (results.next()) {
                rowHandler.processRow(results);
            }
        } catch (SQLException e) {
            throw new MetaDataAccessException(e);
        } finally {
            try {
                if (results!=null) results.close();
            } catch (SQLException e) {
                log.warn("could not close the ResultSet", e);
            }
            try {
                if (connection!=null) connection.close();
            } catch (SQLException e) {
                throw new CleanupFailureDataAccessException("could not close Connection", e);
            }
        }
    }

    public static class MetaDataAccessException extends UncategorizedDataAccessException {
        public MetaDataAccessException(Throwable e) {
            super("could not read metadata", e);
        }
    }

    private Connection getConnection() {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            throw new CannotGetJdbcConnectionException("could not get connection", e);
        }
        return connection;
    }

    protected abstract ResultSet getResults(DatabaseMetaData metaData) throws SQLException;
}

