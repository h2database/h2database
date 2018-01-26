# H2

Welcome to H2, the Java SQL database. The main features of H2 are:

* Very fast, open source, JDBC API
* Embedded and server modes; in-memory databases
* Browser based Console application
* Small footprint: around 1.5 MB jar file size

## Experimental Building & Testing with Maven

### Building

H2 uses [Maven Wrapper](https://github.com/takari/maven-wrapper) setup, you can instruct users to run wrapper scripts:

> $ ./mvnw clean test

or

> $ ./mvnw.cmd clean test

### Running

You can run the server like this

```
mvn exec:java -Dexec.mainClass=org.h2.tools.Server  
```