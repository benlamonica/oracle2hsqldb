The goal is to have a java based migration tool that can transform a Oracle Schema (structure and data) into a hsqldb file (mainly for testing purposes).

The tool will have a ant build command to assist it's execution.

The base of this code was the <a href='http://schemamule.sourceforge.net/'>SchemaMule</a> project (which appears to be inactive) that translates a schema (only structure) from Oracle to Hsqldb. The code was already upgraded to suport BLOB and Raw types. The next step will be data migration.