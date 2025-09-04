# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the `find-datalad-repos` project - a Python package that discovers and tracks DataLad repositories across multiple hosting platforms (GitHub, OSF, GIN, hub.datalad.org, ATRIS). The tool searches for repositories that are either DataLad datasets or have used `datalad run` commands, generating comprehensive reports and maintaining an up-to-date registry.

## Development Commands

### Virtual Environment
Use the available development virtual environment:
```bash
source venvs/dev-pycharm/bin/activate
```

### Testing & Quality Assurance
```bash
# Run all tox environments (lint + typing)
tox

# Run only linting
tox -e lint

# Run only type checking
tox -e typing

# Run tests (currently commented out in tox.ini)
tox -e py3
```

### Running the Tool
```bash
# Search for repositories across all hosts
tox -e run

# Search specific hosts (github,osf,gin,hub.datalad.org,atris)
tox -e run -- --hosts github,gin

# Generate diff reports
tox -e diff
```

### Direct Commands
```bash
# Main command
find-datalad-repos

# Diff command
diff-datalad-repos
```

## Architecture

### Core Components

- **`core.py`**: Defines abstract base classes `Updater` and `Searcher` with generic types for different repository hosts
- **`record.py`**: Central `RepoRecord` model that manages collections of repositories from all hosts
- **Host-specific modules**: Each platform has its own updater implementation:
  - `github.py`: GitHub API integration with abuse detection handling
  - `gin.py`: GIN platform support
  - `osf.py`: Open Science Framework integration
- **`tables.py`**: Report generation and markdown table formatting
- **`readmes.py`**: README file generation for discovered repositories
- **`util.py`**: Shared utilities including Git operations and status management

### Data Flow

1. **Search**: Host-specific `Searcher` classes query APIs for DataLad repositories
2. **Collection**: `Updater` classes process search results and update repository records
3. **Storage**: All discoveries are stored in `datalad-repos.json`
4. **Reporting**: Generate markdown tables and individual README files in `READMEs/` directory

### Key Features

- **Multi-platform support**: Searches GitHub, OSF, GIN, hub.datalad.org, and ATRIS
- **DataLad detection**: Identifies both DataLad datasets and repositories using `datalad run`
- **Rate limiting**: Built-in delays and abuse detection handling for GitHub API
- **Automated reporting**: Generates comprehensive markdown reports and individual repository summaries
- **Registry integration**: Outputs consumed by https://registry.datalad.org/

## Configuration

- **Environment variables**: `GIN_TOKEN`, `GITHUB_TOKEN`, `HUB_DATALAD_ORG_TOKEN`
- **Organization grouping**: Maintained in `github-orgs.json` with group classification ("ours" vs "public")
- **Output files**: `datalad-repos.json` (data), `README.md` (main report), `READMEs/` (individual reports)

## Type System

The codebase uses a sophisticated generic type system with `TypeVar`s (`T`, `U`, `S`) to ensure type safety across different repository host implementations while maintaining a consistent abstract interface.
