# oracle2hsqldb
Automatically exported from code.google.com/p/oracle2hsqldb

This project will convert an Oracle schema into an HSQLDB schema. This is useful to have a test database that is easily embeddable for regression testing, especially now that HSQLDB has an Oracle compatibility mode.

I forked this from the google code project oracle2hsqldb, which in turn was forked from the long abandoned SchemaMule project. I upgraded it to use more modern libraries. The license is the same as the original project, LGPL.

**NOTE:** You will need to install the ojdbc6 jar to your local repository before you can build this project (because Oracle refuses to have a maven repository that is actually usable...)

<pre>mvn -P install-ojdbc6 install:install-file -Dojdbc6.location=&lt;path_to_jar&gt; -Dojdbc6.version=&lt;version&gt;</pre>

Once you have uploaded the ojdbc6 jar, you'll need to change the version in the pom.xml to match the version you uploaded.
