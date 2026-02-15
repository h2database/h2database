# H2 Database - Quick Start Guide for New Engineers

Welcome to the H2 Database project! This guide will get you up and running in minutes.

## 🚀 Quick Setup (5 minutes)

### Prerequisites
- Java JDK 11 or later
- Git

### Get Started

```bash
# 1. Clone the repository
git clone https://github.com/h2database/h2database.git
cd h2database/h2

# 2. Build H2
./build.sh compile

# 3. Run tests
./build.sh test
```

That's it! You're ready to develop.

## 📁 Project Layout

```
h2database/
├── h2/                      # Main H2 directory (work here)
│   ├── src/main/            # Source code
│   ├── src/test/            # Tests
│   ├── build.sh/bat         # Build scripts
│   └── pom.xml              # Maven config
└── Documentation files
```

## 🔨 Common Tasks

### Build the JAR
```bash
./build.sh jar
```
Output: `bin/h2-*.jar`

### Run Tests
```bash
# All tests
./build.sh test

# Quick tests only
./build.sh compile
java -cp "temp:bin/h2-*.jar" org.h2.test.TestAll quick

# Specific test
java -cp "temp:bin/h2-*.jar" org.h2.test.db.TestIndex
```

### Run H2 Console
```bash
java -jar bin/h2-*.jar
# Opens web console at http://localhost:8082
```

### Make a Change

```bash
# 1. Create a branch
git checkout -b fix/my-fix

# 2. Edit code in src/main/org/h2/...

# 3. Build and test
./build.sh compile
./build.sh test

# 4. Commit
git add .
git commit -m "Fix: description"
git push origin fix/my-fix
```

## 🗺️ Where to Find Things

| What you need | Where to look |
|--------------|---------------|
| JDBC driver | `src/main/org/h2/jdbc/` |
| SQL parser | `src/main/org/h2/command/Parser.java` |
| Storage engine | `src/main/org/h2/mvstore/` |
| Built-in functions | `src/main/org/h2/expression/function/` |
| Tests | `src/test/org/h2/test/` |
| Build system | `src/tools/org/h2/build/` |

## 📚 Key Classes to Understand

1. **`Database`** (`org.h2.engine.Database`) - Root database object
2. **`Session`** (`org.h2.engine.Session`) - User session
3. **`Parser`** (`org.h2.command.Parser`) - SQL parser
4. **`Command`** (`org.h2.command.Command`) - Executable SQL command
5. **`Table`** (`org.h2.table.Table`) - Table abstraction
6. **`Value`** (`org.h2.value.Value`) - Data values

## 🐛 Debugging Tips

### Enable SQL tracing
```java
Connection conn = DriverManager.getConnection("jdbc:h2:mem:test;TRACE_LEVEL_FILE=4");
// Check test.trace.db for SQL execution details
```

### Run single test in debug mode
1. Open test class in IDE (e.g., `TestIndex.java`)
2. Set breakpoints
3. Right-click → Debug

### Print debug info
```java
System.out.println("Debug: " + value);
```

## 🎯 First Contribution Ideas

Start with something small:

1. **Fix a typo** in documentation or comments
2. **Add a test case** for existing functionality
3. **Improve error message** to be more helpful
4. **Fix a small bug** from the issue tracker (look for "good first issue" label)

## 📖 Full Documentation

For detailed information, see:

- **[CONTRIBUTING.md](CONTRIBUTING.md)** - Contribution guidelines and workflow
- **[DEVELOPMENT.md](DEVELOPMENT.md)** - Complete development environment setup
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Deep dive into H2's architecture
- **[TESTING.md](TESTING.md)** - Comprehensive testing guide

## 💡 Tips for Success

1. **Read existing code** - H2's code is well-structured and commented
2. **Write tests first** - TDD helps ensure your code works
3. **Keep changes small** - Easier to review and less likely to break things
4. **Ask questions** - Use the mailing list or GitHub discussions
5. **Be patient** - Understanding a database engine takes time

## 🆘 Getting Help

- **Documentation**: [h2database.com](https://h2database.com)
- **Issues**: [GitHub Issues](https://github.com/h2database/h2database/issues)
- **Discussions**: [Mailing List](https://groups.google.com/g/h2-database)
- **Stack Overflow**: ['h2' tag](https://stackoverflow.com/questions/tagged/h2)

## ✅ Checklist for New Engineers

- [ ] Clone repository
- [ ] Build successfully (`./build.sh compile`)
- [ ] Run tests successfully (`./build.sh test`)
- [ ] Run H2 Console (`java -jar bin/h2-*.jar`)
- [ ] Read ARCHITECTURE.md to understand structure
- [ ] Set up IDE (IntelliJ/Eclipse/VS Code)
- [ ] Pick a first issue to work on
- [ ] Join the mailing list for updates

## 🎉 You're Ready!

You now have:
- ✅ Working H2 build
- ✅ Passing tests
- ✅ Understanding of project structure
- ✅ Knowledge of where to find things

Happy coding! Welcome to the H2 community! 🚀
