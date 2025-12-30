# Contributing to BI Market Visibility

Thank you for your interest in contributing! This is primarily a portfolio project, but feedback and suggestions are always welcome.

## 🤝 How to Contribute

### Reporting Bugs
1. Check existing issues to avoid duplicates
2. Use the **Bug Report** template
3. Include detailed reproduction steps
4. Provide environment details

### Suggesting Features
1. Use the **Feature Request** template
2. Explain the problem you're trying to solve
3. Describe your proposed solution
4. Consider implementation impact

### Code Contributions

#### Getting Started
1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/BI_Market_Visibility.git
   ```
3. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

#### Development Workflow
1. Make your changes
2. Write/update tests
3. Run tests locally:
   ```bash
   pytest src/tests/ -v
   ```
4. Check code style:
   ```bash
   black src/
   flake8 src/
   ```
5. Commit with clear messages:
   ```bash
   git commit -m "feat: Add new feature description"
   ```

#### Commit Message Convention
We follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` New features
- `fix:` Bug fixes
- `docs:` Documentation changes
- `style:` Code style changes (formatting)
- `refactor:` Code refactoring
- `test:` Test additions/updates
- `chore:` Maintenance tasks
- `ci:` CI/CD changes

#### Pull Request Process
1. Update documentation if needed
2. Ensure all tests pass
3. Update README.md if adding features
4. Submit PR with clear description
5. Link related issues
6. Wait for review

## 📋 Code Style

- **Python:** Follow PEP 8
- **Formatting:** Use `black` (line length: 100)
- **Imports:** Sort with `isort`
- **Type hints:** Encouraged for functions
- **Docstrings:** Use Google style

### Example:
```python
def process_sales_data(df: DataFrame, date_col: str) -> DataFrame:
    """
    Process sales data with date filtering.
    
    Args:
        df: Input DataFrame with sales data
        date_col: Name of the date column
        
    Returns:
        Processed DataFrame with validated sales data
        
    Raises:
        ValueError: If date_col not found in DataFrame
    """
    # Implementation here
    pass
```

## 🧪 Testing

- Write unit tests for all new functions
- Aim for >80% code coverage
- Use pytest fixtures for test data
- Mock external dependencies

## 📚 Documentation

- Update README.md for user-facing changes
- Update data dictionary for schema changes
- Add docstrings to all functions
- Include examples in documentation

## 🔍 Code Review

All submissions require review. We use GitHub PRs for this purpose.

**Review Criteria:**
- Code quality and readability
- Test coverage
- Documentation completeness
- Performance considerations
- Security best practices

## 📧 Questions?

Feel free to open an issue for:
- Clarification on project setup
- Architecture questions
- Feature discussions
- General feedback

## 📜 License

By contributing, you agree that your contributions will be licensed under the MIT License.

---

Thank you for helping improve this project! 🚀
