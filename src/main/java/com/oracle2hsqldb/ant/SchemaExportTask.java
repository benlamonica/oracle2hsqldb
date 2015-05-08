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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.tools.ant.BuildException;

import com.oracle2hsqldb.Configuration;
import com.oracle2hsqldb.Index;
import com.oracle2hsqldb.Schema;
import com.oracle2hsqldb.SchemaWriter;
import com.oracle2hsqldb.Sequence;
import com.oracle2hsqldb.Table;
import com.oracle2hsqldb.dialect.Dialect;

/**
 * @author Rhett Sutphin
 */
public class SchemaExportTask extends SchemaTask {
    private Dialect dialect;
    private Class dialectClass;
    private File file;

    public void validate() throws BuildException {
        super.validate();
        if (dialectClass == null) throw new BuildException("dialect is required");
    }

    public void execute() throws BuildException {
        validate();
        initDialect();
        try {
            Schema[] schemas = readSchemas();
            SchemaWriter schemaWriter = new SchemaWriter(new Configuration(true, true, true, dialect));
            FileWriter fileWriter = new FileWriter(file);
            for (Schema schema : schemas) {
                for (Table t : schema.tables()) {
                    fileWriter.write(schemaWriter.write(t));
                    fileWriter.write(";\n");
                    for (Index i : t.indicies()) {
                    	fileWriter.write(schemaWriter.write(i));
                    	fileWriter.write(";\n");
                    }
                }
                for (Sequence seq : schema.sequences()) {
                    fileWriter.write(schemaWriter.write(seq));
                    fileWriter.write(";\n");
                }                
            }
            fileWriter.close();
        } catch (SQLException e) {
            throw new BuildException(e);
        } catch (IOException e) {
            throw new BuildException(e);
        }
    }

    public void setFile(File file) {
        this.file = file;
    }

    public void setDialectClass(String dialectClassName) {
        this.dialect = null;
        try {
            this.dialectClass = Class.forName(dialectClassName, true, getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new BuildException(e);
        }
    }

    private void initDialect() throws BuildException {
        if (dialect == null && dialectClass != null) {
            if (!Dialect.class.isAssignableFrom(dialectClass)) {
                throw new BuildException(dialectClass.getName() + " is not an implementation of " + Dialect.class.getName());
            }
            try {
                dialect = (Dialect) dialectClass.newInstance();
            } catch (InstantiationException e) {
                throw new BuildException("Could not create an instance of " + dialectClass.getName(), e);
            } catch (IllegalAccessException e) {
                throw new BuildException("Could not create an instance of " + dialectClass.getName(), e);
            }
        }
    }
}
