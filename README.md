# pentaho-cassandra-plugin
_Cassandra plugin for PDI_

## Building the project

### Pre-requisites for building the project:
* Maven, version 3+
* Java JDK 11
* This [settings.xml](https://raw.githubusercontent.com/pentaho/maven-parent-poms/master/maven-support-files/settings.xml) in your &lt;user-home&gt;/.m2 directory

### Maven commands

Execute from the root directory to build non-obfuscated artifacts:
```
$ mvn clean install
```
To skip tests, specify the `DskipTests` option.


## Generated Artifacts

The `assemblies/plugin` module generates the full plugin assemblies that can be extracted directly into the `{pdi-*-client/data-integration/plugin` folder, in order to enable the cassandra steps on the PDI client:

-------

Licensed under the Apache License, Version 2.0. See LICENSE.txt for more information.
