# Contributing to H2 Database

Welcome to H2! We appreciate your interest in contributing to this project. This guide will help you get started with contributing to the H2 Database Engine.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How Can I Contribute?](#how-can-i-contribute)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Coding Standards](#coding-standards)
- [Submitting Changes](#submitting-changes)
- [Reporting Bugs](#reporting-bugs)
- [Suggesting Enhancements](#suggesting-enhancements)

## Code of Conduct

Be respectful and inclusive. We expect all contributors to:
- Use welcoming and inclusive language
- Be respectful of differing viewpoints and experiences
- Gracefully accept constructive criticism
- Focus on what is best for the community
- Show empathy towards other community members

## How Can I Contribute?

There are many ways to contribute to H2:

### Bug Reports
- Check if the bug has already been reported in the [issue tracker](https://github.com/h2database/h2database/issues)
- If not, create a new issue with a clear title and description
- Include steps to reproduce, expected behavior, and actual behavior
- Add relevant logs, stack traces, or screenshots

### Feature Requests
- Check existing issues and discussions first
- Describe the feature and its use case clearly
- Explain why it would be useful to most H2 users

### Code Contributions
- Fix bugs
- Implement new features
- Improve performance
- Enhance documentation
- Write tests

### Documentation
- Fix typos and clarify existing documentation
- Add examples and tutorials
- Improve API documentation
- Translate documentation

## Getting Started

Before you start contributing, make sure you have:

1. **Java Development Kit (JDK)**: Java 11 or later is required
2. **Build Tools**: H2 uses a custom build system (see [DEVELOPMENT.md](DEVELOPMENT.md))
3. **Git**: For version control
4. **IDE** (optional): IntelliJ IDEA, Eclipse, or your preferred Java IDE

See [DEVELOPMENT.md](DEVELOPMENT.md) for detailed setup instructions.

## Development Workflow

1. **Fork the Repository**
   ```bash
   # Fork the repository on GitHub, then clone your fork
   git clone https://github.com/YOUR_USERNAME/h2database.git
   cd h2database
   ```

2. **Create a Branch**
   ```bash
   git checkout -b feature/my-feature-name
   # or
   git checkout -b fix/bug-description
   ```

3. **Make Your Changes**
   - Write clean, maintainable code
   - Follow the existing code style
   - Add tests for your changes
   - Update documentation as needed

4. **Test Your Changes**
   ```bash
   cd h2
   ./build.sh test
   ```

5. **Commit Your Changes**
   ```bash
   git add .
   git commit -m "Brief description of changes"
   ```
   
   Write clear commit messages:
   - Use present tense ("Add feature" not "Added feature")
   - Keep the first line under 72 characters
   - Reference issue numbers when applicable

6. **Push and Create Pull Request**
   ```bash
   git push origin feature/my-feature-name
   ```
   Then create a pull request on GitHub.

## Coding Standards

### General Guidelines

- **Code Style**: Follow the existing code style in the project
- **Indentation**: Use 4 spaces (no tabs)
- **Line Length**: Keep lines under 120 characters when practical
- **Naming Conventions**:
  - Classes: `PascalCase` (e.g., `DatabaseEngine`)
  - Methods/Variables: `camelCase` (e.g., `executeQuery`)
  - Constants: `UPPER_SNAKE_CASE` (e.g., `DEFAULT_PORT`)
  - Private fields: Consider using prefix when it improves clarity

### Java-Specific Guidelines

- Use Java 11+ features appropriately
- Prefer Java standard library over third-party dependencies
- Use meaningful variable and method names
- Avoid unnecessary comments; code should be self-documenting when possible
- Add JavaDoc for public APIs
- Handle exceptions appropriately
- Close resources properly (use try-with-resources)

### Testing Guidelines

- Write tests for all new features and bug fixes
- Follow existing test patterns in the codebase
- Tests should be:
  - Isolated (independent of each other)
  - Repeatable (same result every time)
  - Fast (avoid slow tests when possible)
  - Comprehensive (cover edge cases)

Example test structure:
```java
public class TestMyFeature extends TestBase {
    
    @Test
    public void testBasicFunctionality() throws Exception {
        // Setup
        Connection conn = getConnection("myTest");
        Statement stat = conn.createStatement();
        
        // Execute
        stat.execute("CREATE TABLE TEST(ID INT)");
        
        // Verify
        ResultSet rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='TEST'");
        assertTrue(rs.next());
        
        // Cleanup
        conn.close();
    }
}
```

### Documentation Guidelines

- Update documentation when changing behavior
- Add JavaDoc for new public APIs
- Include code examples for complex features
- Keep README and other docs up to date

## Submitting Changes

### Pull Request Process

1. **Before Submitting**:
   - Ensure all tests pass
   - Update documentation
   - Rebase on latest main branch if needed
   - Verify your changes solve the intended problem

2. **Pull Request Description**:
   - Clearly describe what the PR does
   - Reference related issues (e.g., "Fixes #123")
   - List any breaking changes
   - Include test results if relevant

3. **Review Process**:
   - Maintainers will review your PR
   - Be responsive to feedback
   - Make requested changes promptly
   - Engage in discussion constructively

4. **After Approval**:
   - Maintainers will merge your PR
   - You may be asked to squash commits

## Reporting Bugs

When reporting bugs, include:

1. **H2 Version**: Check with `SELECT H2VERSION()` or the jar file name
2. **Java Version**: Output of `java -version`
3. **Operating System**: Windows, Linux, macOS, etc.
4. **Database Mode**: If using a specific compatibility mode
5. **Steps to Reproduce**: Minimal code to reproduce the issue
6. **Expected Behavior**: What you expected to happen
7. **Actual Behavior**: What actually happened
8. **Error Messages**: Complete stack traces and error messages
9. **Configuration**: Any non-default settings

### Minimal Reproducible Example

```java
import java.sql.*;

public class TestCase {
    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:test");
        Statement stat = conn.createStatement();
        
        // Steps that reproduce the bug
        stat.execute("...");
        
        conn.close();
    }
}
```

## Suggesting Enhancements

For feature requests:

1. **Search First**: Check if someone already suggested it
2. **Use Case**: Describe the problem you're trying to solve
3. **Proposed Solution**: How you envision the feature working
4. **Alternatives**: Other approaches you've considered
5. **Examples**: Provide examples of usage
6. **Backward Compatibility**: Consider impact on existing users

## Additional Resources

- [Development Setup Guide](DEVELOPMENT.md)
- [Architecture Overview](ARCHITECTURE.md)
- [Testing Guide](TESTING.md)
- [H2 Website](https://h2database.com)
- [Issue Tracker](https://github.com/h2database/h2database/issues)
- [Mailing List](https://groups.google.com/g/h2-database)

## Questions?

If you have questions:
- Check the [documentation](https://h2database.com/html/main.html)
- Ask on the [mailing list](https://groups.google.com/g/h2-database)
- Create a discussion on GitHub

Thank you for contributing to H2!
