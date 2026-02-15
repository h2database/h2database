# H2 Database - Development Guide

This guide will help you set up your development environment and start working on the H2 Database Engine.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Getting the Source Code](#getting-the-source-code)
- [Development Environment Setup](#development-environment-setup)
- [Building H2](#building-h2)
- [Running Tests](#running-tests)
- [IDE Setup](#ide-setup)
- [Project Structure](#project-structure)
- [Development Tools](#development-tools)
- [Common Development Tasks](#common-development-tasks)
- [Troubleshooting](#troubleshooting)

## Prerequisites

### Required Software

1. **Java Development Kit (JDK) 11 or later**
   - H2 requires JDK 11 as the minimum version
   - JDK 17 is recommended for development
   - Download from [Adoptium](https://adoptium.net/) or [Oracle](https://www.oracle.com/java/technologies/downloads/)

   Verify installation:
   ```bash
   java -version
   javac -version
   ```

2. **Git**
   - For version control
   - Download from [git-scm.com](https://git-scm.com/)

3. **JAVA_HOME Environment Variable**
   - Must be set to your JDK installation directory
   
   On Linux/macOS:
   ```bash
   export JAVA_HOME=/path/to/jdk
   export PATH=$JAVA_HOME/bin:$PATH
   ```
   
   On Windows:
   ```cmd
   set JAVA_HOME=C:\path\to\jdk
   set PATH=%JAVA_HOME%\bin;%PATH%
   ```

### Optional Tools

- **Maven 3.6+**: For Maven-based builds (experimental)
- **IDE**: IntelliJ IDEA, Eclipse, or VS Code
- **Git GUI**: GitKraken, SourceTree, or GitHub Desktop

## Getting the Source Code

1. **Fork the Repository** (if contributing)
   - Go to https://github.com/h2database/h2database
   - Click "Fork" button

2. **Clone the Repository**
   ```bash
   # Clone your fork
   git clone https://github.com/YOUR_USERNAME/h2database.git
   cd h2database
   
   # Or clone the main repository
   git clone https://github.com/h2database/h2database.git
   cd h2database
   ```

3. **Add Upstream Remote** (if you forked)
   ```bash
   git remote add upstream https://github.com/h2database/h2database.git
   ```

## Development Environment Setup

H2 uses a custom build system written in Java. The main build directory is `h2/`.

### Initial Setup

```bash
cd h2
```

All subsequent commands should be run from the `h2` directory.

## Building H2

### Using the Custom Build System (Recommended)

H2's custom build system provides various build targets.

**On Linux/macOS:**
```bash
./build.sh
```

**On Windows:**
```cmd
build.bat
```

### Available Build Targets

View all available targets:
```bash
./build.sh
```

Common targets:

- **`clean`** - Remove all generated files
  ```bash
  ./build.sh clean
  ```

- **`compile`** - Compile all Java source files
  ```bash
  ./build.sh compile
  ```

- **`jar`** - Build the H2 JAR file (h2-*.jar)
  ```bash
  ./build.sh jar
  ```

- **`test`** - Run all tests
  ```bash
  ./build.sh test
  ```

- **`testCI`** - Run tests for continuous integration
  ```bash
  ./build.sh testCI
  ```

- **`docs`** - Generate documentation
  ```bash
  ./build.sh docs
  ```

- **`javadoc`** - Generate API documentation
  ```bash
  ./build.sh javadoc
  ```

### Building with Maven (Experimental)

Maven support is experimental. You must first compile using the custom build system:

```bash
# First, compile with the custom build system
./build.sh compile

# Then you can use Maven
./mvnw clean package -Dmaven.test.skip=true
```

Or with Maven installed:
```bash
mvn clean package -Dmaven.test.skip=true
```

**Note**: The Maven-generated JAR is larger than the official build (due to different packaging) and lacks some features (OSGi attributes, complete native-image configuration). Use the custom build system for production builds.

**When to use each build system:**
- **Custom build system (recommended)**: For production builds, official releases, or when you need full feature parity with official H2 builds
- **Maven**: For IDE integration, dependency management in Maven-based projects, or when you're already familiar with Maven workflows

## Running Tests

### Run All Tests

```bash
./build.sh test
```

This runs the comprehensive test suite. It may take several minutes.

### Run CI Tests

```bash
./build.sh testCI
```

Runs tests suitable for continuous integration (faster, focuses on core functionality).

### Run Specific Tests

You can run specific test classes:

```bash
cd h2
# Compile first
./build.sh compile

# Run a specific test
java -cp "temp:bin/h2-*.jar" org.h2.test.TestAll [options]
```

Common test options:
- `quick` - Run quick tests only
- `stopOnError` - Stop on first error
- `tiny` - Run minimal test set
- `big` - Run extended test set

Example:
```bash
java -cp "temp:bin/h2-*.jar" org.h2.test.TestAll quick
```

### Understanding Test Output

Tests print progress and results:
- `.` - Test passed
- `E` - Error occurred
- `F` - Test failed

At the end, you'll see a summary:
```
Test cases: 1234
Errors: 0
Failures: 0
Time: 45.2 seconds
```

## IDE Setup

### IntelliJ IDEA

1. **Import Project**
   - Open IntelliJ IDEA
   - Select "Open" and choose the `h2database` directory
   - IDEA will detect the project structure

2. **Configure JDK**
   - Go to File → Project Structure → Project
   - Set Project SDK to JDK 11 or later
   - Set Project language level to 11

3. **Build the Project**
   - First build using the custom build script:
     ```bash
     cd h2
     ./build.sh compile
     ```
   - Then refresh the project in IntelliJ

4. **Run Tests from IDE**
   - Navigate to a test class (e.g., `TestAll.java`)
   - Right-click → Run
   - Or use the Maven plugin (experimental)

### Eclipse

1. **Import Project**
   - File → Import → Existing Projects into Workspace
   - Select the `h2database` directory

2. **Configure JDK**
   - Right-click project → Properties → Java Build Path
   - Add JDK 11+ to libraries

3. **Build**
   - Use the custom build script first:
     ```bash
     cd h2
     ./build.sh compile
     ```
   - Then refresh the project in Eclipse

### VS Code

1. **Open Folder**
   - Open the `h2database` folder in VS Code

2. **Install Extensions**
   - Java Extension Pack (Microsoft)
   - Maven for Java

3. **Configure**
   - VS Code should auto-detect the Java project
   - Use the integrated terminal for custom builds

## Project Structure

```
h2database/
├── h2/                          # Main H2 directory
│   ├── src/
│   │   ├── main/                # Main source code
│   │   │   ├── org/h2/         # Core H2 packages
│   │   │   │   ├── api/        # JDBC API implementations
│   │   │   │   ├── command/    # SQL command processing
│   │   │   │   ├── engine/     # Database engine core
│   │   │   │   ├── expression/ # SQL expression handling
│   │   │   │   ├── index/      # Index implementations
│   │   │   │   ├── jdbc/       # JDBC driver
│   │   │   │   ├── mvstore/    # MVStore storage engine
│   │   │   │   ├── schema/     # Schema objects
│   │   │   │   ├── server/     # Server implementations
│   │   │   │   ├── table/      # Table implementations
│   │   │   │   ├── tools/      # H2 tools (Console, Shell, etc.)
│   │   │   │   └── value/      # Data type values
│   │   │   └── META-INF/       # Service providers, manifests
│   │   ├── test/                # Test source code
│   │   │   └── org/h2/test/    # Test suites
│   │   ├── tools/               # Build tools
│   │   │   └── org/h2/build/   # Custom build system
│   │   ├── docsrc/              # Documentation sources
│   │   └── installer/           # Installer resources
│   ├── build.sh                 # Build script (Linux/macOS)
│   ├── build.bat                # Build script (Windows)
│   ├── pom.xml                  # Maven configuration (experimental)
│   └── mvnw                     # Maven wrapper
├── README.md                    # Main readme
├── CONTRIBUTING.md              # Contribution guidelines
├── DEVELOPMENT.md               # This file
├── ARCHITECTURE.md              # Architecture overview
├── TESTING.md                   # Testing guidelines
└── LICENSE.txt                  # License information
```

### Key Directories

- **`src/main/org/h2/`** - All production code
- **`src/test/org/h2/test/`** - All test code
- **`src/tools/org/h2/build/`** - Build system code
- **`src/docsrc/`** - Documentation source files
- **`temp/`** - Compiled classes (generated)
- **`bin/`** - Build outputs, JAR files (generated)
- **`docs/`** - Generated documentation (generated)

## Development Tools

### Build System

The custom build system (`Build.java`) provides:
- Dependency management (automatic download)
- Compilation
- Testing
- Documentation generation
- JAR creation
- Code coverage

### Running the H2 Console

After building:
```bash
java -jar bin/h2-*.jar
```

Or:
```bash
java -cp bin/h2-*.jar org.h2.tools.Console
```

This starts the web-based H2 Console at http://localhost:8082

### Running the H2 Server

Start the TCP server:
```bash
java -cp bin/h2-*.jar org.h2.tools.Server
```

With options:
```bash
java -cp bin/h2-*.jar org.h2.tools.Server -tcp -tcpPort 9092 -web -webPort 8082
```

### Running the H2 Shell

Interactive SQL shell:
```bash
java -cp bin/h2-*.jar org.h2.tools.Shell
```

## Common Development Tasks

### 1. Making a Code Change

```bash
# 1. Create a branch
git checkout -b fix/my-bug-fix

# 2. Edit code in your IDE or editor

# 3. Build and test
cd h2
./build.sh compile
./build.sh test

# 4. Commit changes
git add .
git commit -m "Fix: description of fix"

# 5. Push and create PR
git push origin fix/my-bug-fix
```

### 2. Adding a New Feature

```bash
# 1. Plan your changes (see ARCHITECTURE.md)

# 2. Write tests first (TDD approach)
# Edit files in src/test/org/h2/test/

# 3. Implement the feature
# Edit files in src/main/org/h2/

# 4. Build and test
./build.sh compile
./build.sh test

# 5. Update documentation
# Edit files in src/docsrc/ if needed

# 6. Build documentation
./build.sh docs
```

### 3. Debugging

**Using IDE Debugger:**
1. Set breakpoints in your IDE
2. Create a run configuration for a test class
3. Run in debug mode

**Using Print Statements:**
```java
System.out.println("Debug: value=" + someValue);
```

**Using H2 Trace:**
```java
// In H2 code
if (trace.isDebugEnabled()) {
    trace.debug("operation {0}", paramValue);
}
```

### 4. Code Coverage

Generate code coverage report:
```bash
./build.sh coverage
```

View the report in `coverage/index.html`

### 5. Generating Documentation

```bash
# Generate all documentation
./build.sh docs

# Generate only Javadocs
./build.sh javadoc
```

Output goes to the `docs/` directory.

## Troubleshooting

### Common Issues

**1. JAVA_HOME not set**
```
Error: JAVA_HOME is not defined.
```

Solution: Set JAVA_HOME environment variable to your JDK location.

**2. Compilation errors after pulling changes**
```bash
# Clean and rebuild
./build.sh clean
./build.sh compile
```

**3. Tests failing**
- Ensure you're on the latest code: `git pull`
- Clean build: `./build.sh clean compile test`
- Check if tests fail on main branch too

**4. OutOfMemoryError during build**
```bash
# Increase heap size
export JAVA_OPTS="-Xmx1g"
./build.sh compile
```

**5. Port already in use (8082)**
```bash
# Change the port
java -cp bin/h2-*.jar org.h2.tools.Console -webPort 8083
```

### Getting Help

- Check existing documentation at [h2database.com](https://h2database.com)
- Search [GitHub issues](https://github.com/h2database/h2database/issues)
- Ask on the [mailing list](https://groups.google.com/g/h2-database)
- Review [ARCHITECTURE.md](ARCHITECTURE.md) for codebase structure

## Next Steps

Once you have your development environment set up:

1. Read [ARCHITECTURE.md](ARCHITECTURE.md) to understand the codebase
2. Review [TESTING.md](TESTING.md) for testing guidelines
3. Check [CONTRIBUTING.md](CONTRIBUTING.md) for contribution workflow
4. Start with a small bug fix or documentation improvement
5. Join the community discussions

Happy coding!
