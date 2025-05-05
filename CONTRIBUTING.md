# Contributing to NL-Cube

Thank you for your interest in contributing to NL-Cube! This document provides guidelines and instructions for contributing to the project. We welcome contributions of all kinds including bug reports, feature requests, documentation improvements, and code changes.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Environment](#development-environment)
- [Contributing Process](#contributing-process)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Documentation](#documentation)
- [Issue Reporting Guidelines](#issue-reporting-guidelines)
- [Communication](#communication)

## Code of Conduct

This project and everyone participating in it is governed by the Rust community Code of Conduct. By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainers.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** to your local machine
3. **Set up the development environment** as described below
4. **Create a branch** for your contribution
5. **Make your changes** following our coding standards
6. **Submit a pull request** from your branch to our `main` branch

## Development Environment

### Prerequisites

- **Rust** (1.84.0 or later)
- **Redis**
- **Git**

### Setup

```bash
# Clone your fork of the repository
git clone https://github.com/joefrost01/nl-cube.git
cd nl-cube

# Add the original repository as a remote
git remote add upstream https://github.com/joefrost01/nl-cube.git

# Create a branch for your work
git checkout -b feature/your-feature-name

# Build the project
cargo build
```

For development mode with template hot-reloading:

```bash
DEV_MODE=true cargo run
```

## Contributing Process

1. **Check existing issues**: Look for existing issues or create a new one to discuss your planned contribution.
2. **Discuss your approach**: For larger changes, discuss your approach with the maintainers before starting work.
3. **Keep changes focused**: Make changes that address a specific concern or feature.
4. **Write good commit messages**: Use clear and descriptive commit messages.
5. **Update documentation**: Update relevant documentation to reflect your changes.
6. **Add tests**: Add tests for new features or bug fixes.
7. **Ensure CI passes**: Make sure all continuous integration checks pass.

## Pull Request Process

1. **Keep PRs small and focused**: A PR should address a single concern.
2. **Follow the PR template**: Fill out the PR template completely.
3. **Update the README.md or documentation**: If necessary, update the documentation to reflect your changes.
4. **Pass all CI checks**: Ensure your code passes all CI checks.
5. **Respond to feedback**: Be responsive to feedback on your PR.
6. **Squash commits**: Before merging, squash your commits into a logical set.

## Coding Standards

We follow Rust's official style guidelines:

- Use `rustfmt` to format your code
- Use `clippy` to catch common mistakes
- Follow [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Write clear, concise comments
- Use meaningful variable and function names
- Keep functions focused on a single responsibility

Additional standards for NL-Cube:

- Use `async/await` consistently for asynchronous code
- Ensure proper error handling
- Add debug and info logging at appropriate points
- Document public interfaces with doc comments

## Testing

- Write tests for all new features and bug fixes
- Run tests locally before pushing changes
- Make sure tests are passing in CI

To run tests:

```bash
# Run all tests
cargo test
```

## Documentation

Documentation is as important as code. Please ensure:

- Public API functions, traits, and structs are documented with doc comments
- Complex algorithms or flows include explanatory comments
- User-facing changes are reflected in appropriate documentation
- README.md is updated as needed

We use Quarto for the main documentation site. Documentation sources are in the `docs/` directory.

## Issue Reporting Guidelines

When reporting issues, please include:

- **Description**: Clear description of the issue
- **Steps to Reproduce**: Detailed steps to reproduce the issue
- **Expected Behavior**: What you expected to happen
- **Actual Behavior**: What actually happened
- **Environment**: Details about your environment (OS, Rust version, etc.)
- **Additional Context**: Any other relevant information

## Communication

- **GitHub Issues**: For bug reports, feature requests, and discussions
- **Pull Requests**: For code contributions
- **Wiki**: For documentation and guides

## Areas for Contribution

We particularly welcome contributions in these areas:

- **Documentation improvements**
- **Test coverage expansion**
- **Performance optimizations**
- **New backend implementations** (e.g., PostgreSQL)
- **UI enhancements**
- **Example workflows and templates**

## License

By contributing to NL-Cube, you agree that your contributions will be licensed under the project's Apache 2 License.

---

Thank you for considering contributing to NL-Cube! Your help is greatly appreciated.
