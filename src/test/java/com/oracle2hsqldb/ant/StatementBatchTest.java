package /*
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

com.oracle2hsqldb.ant;

import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;

import org.easymock.EasyMock;

/**
 * @author Moses Hohman
 */
public class StatementBatchTest extends TestCase {
    private Statement statement;
    private StatementBatch batch;

    protected void setUp() throws Exception {
        statement = EasyMock.createMock(Statement.class);
    }

    private static final String SQL = "SELECT 1 FROM DUAL";

    public void testStatementDirectlyExecutedIfBatchSizeIsOneOrLess() throws SQLException {
        batch = new StatementBatch(statement, 1);
        EasyMock.expect(statement.executeUpdate(SQL)).andReturn(0);
        EasyMock.replay(statement);
        batch.executeUpdate(SQL);
        EasyMock.verify(statement);
    }

    public void testStatementAddedToBatchIfBatchSizeNotReached() throws SQLException {
        batch = new StatementBatch(statement, 2);
        statement.addBatch(SQL);
        EasyMock.replay(statement);
        batch.executeUpdate(SQL);
        EasyMock.verify(statement);
    }

    public void testBatchExecutedIfBatchSizeReached() throws SQLException {
        String firstSql = "SELECT 1 FROM DUAL";
        String secondSql = "SELECT 2 FROM DUAL";
        batch = new StatementBatch(statement, 2);
        statement.addBatch(firstSql);
        statement.addBatch(secondSql);
        EasyMock.expect(statement.executeBatch()).andReturn(new int[0]);
        EasyMock.replay(statement);
        batch.executeUpdate(firstSql);
        batch.executeUpdate(secondSql);
        EasyMock.verify(statement);
    }

    public void testBatchCountRollsOverProperly() throws SQLException {
        batch = new StatementBatch(statement, 2);
        statement.addBatch(SQL);
        statement.addBatch(SQL);
        EasyMock.expect(statement.executeBatch()).andReturn(new int[0]);
        statement.addBatch(SQL);
        statement.addBatch(SQL);
        EasyMock.expect(statement.executeBatch()).andReturn(new int[0]);
        statement.addBatch(SQL);
        EasyMock.replay(statement);
        batch.executeUpdate(SQL);
        batch.executeUpdate(SQL);
        batch.executeUpdate(SQL);
        batch.executeUpdate(SQL);
        batch.executeUpdate(SQL);
        EasyMock.verify(statement);
    }

    public void testFlushExecutesBatchIfThereAreBatchedStatementsRegardlessOfCount() throws SQLException {
        batch = new StatementBatch(statement, 2);
        statement.addBatch(SQL);
        EasyMock.expect(statement.executeBatch()).andReturn(new int[0]);
        EasyMock.replay(statement);
        batch.executeUpdate(SQL);
        batch.flush();
        EasyMock.verify(statement);
    }

    public void testFlushDoesNothingIfNothingIsBatched() throws SQLException {
        batch = new StatementBatch(statement, 1);
        EasyMock.replay(statement);
        batch.flush();
        EasyMock.verify(statement);
    }

    public void testCloseClosesUnderlyingStatement() throws SQLException {
        batch = new StatementBatch(statement, 1);
        statement.close();
        EasyMock.replay(statement);
        batch.close();
        EasyMock.verify(statement);
    }
}
