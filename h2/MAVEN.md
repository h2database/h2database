# H2

Welcome to H2, the Java SQL database. The main features of H2 are:

* Very fast, open source, JDBC API
* Embedded and server modes; in-memory databases
* Browser based Console application
* Small footprint: around 2.5 MB jar file size

## Experimental Building & Testing with Maven

### Preparation

Use non-Maven build to create all necessary resources:

```Batchfile
./build.cmd compile
```

or

```sh
./build.sh compile
```

### Building

To build only the database jar use

```sh
mvn -Dmaven.test.skip=true package
```

If you don't have Maven installed use included [Maven Wrapper](https://github.com/takari/maven-wrapper) setup:

```sh
./mvnw -Dmaven.test.skip=true package
```

or

```Batchfile
./mvnw.cmd -Dmaven.test.skip=true package
```

Please note that jar generated with Maven is larger than official one and it does not include OSGi attributes.
Use build script with `jar` target instead if you need a compatible jar.

### Testing

To run the tests use

```sh
mvn clean test
```

### Running

You can run the server like this

```sh
mvn exec:java -Dexec.mainClass=org.h2.tools.Server  
```
