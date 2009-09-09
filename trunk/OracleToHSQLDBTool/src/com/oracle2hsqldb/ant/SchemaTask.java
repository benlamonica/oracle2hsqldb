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

import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.BuildException;

import com.oracle2hsqldb.Schema;

import java.sql.SQLException;
import java.util.List;
import java.util.LinkedList;

/**
 * @author Rhett Sutphin
 */
public abstract class SchemaTask extends Task implements Validatable {
    protected List froms;

    public SchemaTask() {
        froms = new LinkedList();
    }

    public void validate() throws BuildException {
        if (froms.isEmpty()) throw new BuildException("No from subelements to copy");
    }

    public void addConfiguredFrom(SchemaParams schema) {
        preprocessAndValidate(schema);
        froms.add(schema);
    }

    protected void preprocessAndValidate(SchemaParams schema) {
        schema.setTask(this);
        schema.validate();
    }

    protected Schema[] readSchemas() throws SQLException {
        Schema[] schemas = new Schema[froms.size()];
        for (int i = 0; i < froms.size(); i++) {
            log(getFrom(i).getSchema()
                    + ": incl" + getFrom(i).getIncludedTables()
                    + " excl" + getFrom(i).getExcludedTables(),
                    Project.MSG_VERBOSE);
            schemas[i] = getFrom(i).readSchema();
        }
        return schemas;
    }

    private SchemaParams getFrom(int index) {
        return (SchemaParams) froms.get(index);
    }
}
