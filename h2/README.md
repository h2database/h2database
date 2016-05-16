

##Development

###Setup JDK

Maven requires at minimum the JDK 1.7 whereas H2 is designed to be compiled against the JDK 1.6; the way in which this is
resolved is through [maven toolchains](https://maven.apache.org/guides/mini/guide-using-toolchains.html).

You simply need to create a `toolchains.xml` file in `~/.m2/` that tells maven where to find the JDK 1.6

Here is a sample file

```
<?xml version="1.0" encoding="UTF8"?>
<toolchains>
  <!-- JDK toolchains -->
  <toolchain>
    <type>jdk</type>
    <provides>
      <version>1.6</version>
    </provides>
    <configuration>
      <jdkHome>/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home/</jdkHome>
    </configuration>
  </toolchain>
</toolchains>
```

###Building

H2 uses [Maven Wrapper](https://github.com/takari/maven-wrapper) setup, you can instruct users to run wrapper scripts:

> $ ./mvnw clean install

or

> $ ./mvnw.cmd clean install