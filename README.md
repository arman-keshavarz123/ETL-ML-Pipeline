# Data Extractor

A configuration-driven ETL (Extract, Transform, Load) pipeline built in Python. Define your entire data pipeline in YAML — no application code changes needed to add new sources, transformations, or destinations.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [CLI Usage](#cli-usage)
- [How the Pipeline Works](#how-the-pipeline-works)
- [Configuration Reference](#configuration-reference)
  - [Pipeline Config](#pipeline-config)
  - [Extractors](#extractors)
  - [Transformers](#transformers)
  - [Loaders](#loaders)
- [Incremental Loading](#incremental-loading)
- [Available Modules](#available-modules)
- [Example Pipelines](#example-pipelines)
- [Testing](#testing)
- [Project Structure](#project-structure)

## Features

- **YAML-driven** — define pipelines entirely through configuration files
- **Plugin architecture** — extractors, transformers, and loaders are auto-discovered via a decorator-based registry
- **Multiple data sources** — REST APIs, local JSON files, web scraping via headless Chromium
- **Data validation** — validate rows against Pydantic models, automatically dropping invalid records
- **Data cleaning** — 11 built-in cleaning rules (dedup, strip whitespace, type casting, date standardization, etc.)
- **Multiple destinations** — local JSON files, SQL databases (SQLite, PostgreSQL) with upsert support
- **Incremental loading** — cursor-based extraction so only new/changed data is pulled on subsequent runs
- **Atomic state persistence** — cursor state is saved only after a successful load, preventing data inconsistency
- **Retry with backoff** — configurable exponential backoff on transient failures

## Architecture

```
                    ┌──────────────────────┐
                    │   Pipeline YAML      │
                    │   (orchestration)    │
                    └──────────┬───────────┘
                               │
                    ┌──────────▼───────────┐
                    │   PipelineEngine     │
                    │   (engine.py)        │
                    └──────────┬───────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                     │
 ┌────────▼────────┐ ┌────────▼────────┐ ┌──────────▼────────┐
 │   Extract       │ │   Transform     │ │   Load            │
 │                 │ │   (chain)       │ │                   │
 │ • REST API      │ │ • Validation    │ │ • JSON file       │
 │ • JSON file     │ │ • Cleaning      │ │ • SQL database    │
 │ • Web scraper   │ │ • Pass-through  │ │   (with upsert)   │
 └─────────────────┘ └─────────────────┘ └───────────────────┘
```

The engine reads a pipeline YAML file, resolves string keys (like `"rest_api"` or `"json_local"`) to concrete Python classes through the **registry**, and executes the Extract → Transform → Load lifecycle. The engine never imports a concrete class directly — everything is discovered via `@register_extractor`, `@register_transformer`, and `@register_loader` decorators at import time.

## Tech Stack

| Tool | Purpose |
|---|---|
| **Python 3.11+** | Runtime |
| **Pandas** | DataFrame backend for all data manipulation between pipeline stages |
| **Pydantic** | Config validation at startup and row-level data validation during transforms |
| **httpx** | HTTP client for REST API extraction (sync, with timeout and auth support) |
| **Playwright** | Headless Chromium browser for web scraping extraction |
| **SQLAlchemy** | Database abstraction for SQL loading (SQLite, PostgreSQL) with dialect-specific upsert |
| **PyYAML** | YAML parsing for all configuration files |
| **pytest** | Test framework (99 tests) |

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/arman-keshavarz123/Data-Extractor.git
cd Data-Extractor
```

### 2. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install the package

```bash
pip install -e ".[dev]"
```

Or install dependencies manually:

```bash
pip install pandas pydantic "pydantic[email]" httpx pyyaml sqlalchemy playwright pytest
```

### 4. Install Playwright browsers (only needed for web scraping)

```bash
playwright install chromium
```

## Quick Start

Run the included demo pipeline that pulls todos from the JSONPlaceholder API, validates them, cleans the data, and saves to a local JSON file:

```bash
python -m data_extractor -c pipeline_config.yaml
```

Check the output:

```bash
cat output/todos.json
```

## CLI Usage

```
usage: data-extractor [-h] [-c CONFIG] [-f] [-l]

Run a configuration-driven ETL pipeline.

options:
  -h, --help            show this help message and exit
  -c, --config CONFIG   Path to the pipeline YAML config file.
  -f, --full-refresh    Ignore the stored cursor and extract everything from
                        scratch. The new cursor is still saved after a
                        successful load.
  -l, --list-modules    List all registered extractors, transformers, and
                        loaders, then exit.
```

### Run a pipeline

```bash
python -m data_extractor -c pipeline_config.yaml
```

### Full refresh (ignore saved cursor)

```bash
python -m data_extractor -c pipeline_config.yaml --full-refresh
```

### List all available modules

```bash
python -m data_extractor --list-modules
```

Output:

```
EXTRACTORS
----------
  rest_api                       RESTAPIExtractor
  json_file                      JSONFileExtractor
  playwright_scraper             PlaywrightScraperExtractor

TRANSFORMERS
------------
  pass_through                   PassThroughTransformer
  pydantic_validation            PydanticValidationTransformer
  data_cleaning                  DataCleaningTransformer

LOADERS
-------
  json_local                     JSONLocalLoader
  sql_database                   SQLAlchemyLoader
```

## How the Pipeline Works

A pipeline runs in three stages, each defined in the YAML config:

### Step 1: Extract

The engine reads the `extract` section, resolves the `source` key to a registered extractor class, and calls `extractor.extract()` which returns a Pandas DataFrame.

**Example** — pulling todos from a REST API:
```yaml
extract:
  source: "rest_api"
  config_file: "configs/sources/jsonplaceholder.yaml"
```

The source config (`jsonplaceholder.yaml`):
```yaml
base_url: "https://jsonplaceholder.typicode.com"
endpoint: "/todos"
pagination: "none"
timeout: 15
```

### Step 2: Transform

The DataFrame passes through a chain of transformers, executed top-to-bottom. Each transformer receives the DataFrame, processes it, and returns a new DataFrame.

```yaml
transform:
  - name: "pydantic_validation"
    config_file: "configs/transforms/pydantic_validation.yaml"
  - name: "data_cleaning"
    config_file: "configs/transforms/data_cleaning.yaml"
```

**Pydantic validation** validates each row against a Pydantic model. Invalid rows are logged as warnings and dropped:

```yaml
model: "data_extractor.schemas.todo.TodoItem"
chunk_size: 1000
strict: false
```

**Data cleaning** applies rules in a fixed order:

```yaml
strip_whitespace: true
deduplicate: true
cast_types:
  userId: "int64"
  id: "int64"
  completed: "bool"
```

### Step 3: Load

The cleaned DataFrame is written to the configured destination.

```yaml
load:
  destination: "json_local"
  config_file: "configs/loaders/json_local.yaml"
```

The loader config:
```yaml
output_path: "output/todos.json"
orient: "records"
indent: 2
```

### Retry and Error Handling

The engine wraps extract and load steps in a retry loop with configurable exponential backoff:

```yaml
settings:
  log_level: "INFO"
  retry:
    max_attempts: 3
    backoff_seconds: 2
  on_failure: "abort"
```

## Configuration Reference

### Pipeline Config

The top-level YAML file orchestrating the pipeline:

```yaml
version: "1.0"

pipeline:
  name: "my_pipeline"              # Unique pipeline name (used for state tracking)
  description: "What this does"

  extract:
    source: "<extractor_key>"      # Registered extractor name
    config_file: "path/to/config.yaml"
    # OR inline config:
    # inline_config:
    #   base_url: "https://..."

  transform:                        # Optional — list of transforms, executed in order
    - name: "<transformer_key>"
      config_file: "path/to/config.yaml"

  load:
    destination: "<loader_key>"    # Registered loader name
    config_file: "path/to/config.yaml"

  incremental:                      # Optional — cursor-based incremental extraction
    cursor_field: "id"
    cursor_param: "since_id"
    initial_value: 0

settings:
  log_level: "INFO"                 # DEBUG, INFO, WARNING, ERROR
  retry:
    max_attempts: 3
    backoff_seconds: 2
  on_failure: "abort"               # abort, skip, warn
  state_file: "state.json"          # Where to persist the incremental cursor
```

### Extractors

#### `rest_api` — REST API Extractor

Fetches data from HTTP APIs using httpx.

```yaml
base_url: "https://api.example.com"
endpoint: "/data"
pagination: "none"                  # "none", "page_param", or "link_header"
timeout: 15
# Optional:
query_params:
  status: "active"
headers:
  Accept: "application/json"
auth_token_env: "MY_API_TOKEN"      # Reads token from environment variable
page_param: "page"                  # For page_param pagination
max_pages: 10                       # Safety limit for pagination
```

#### `json_file` — Local JSON File Extractor

Reads a local JSON file into a DataFrame.

```yaml
file_path: "data/input.json"
orient: "records"                   # Pandas JSON orient (records, columns, index, etc.)
```

#### `playwright_scraper` — Web Scraper Extractor

Uses headless Chromium to scrape web pages via CSS selectors.

```yaml
url: "https://books.toscrape.com/"
wait_for: "article.product_pod"     # CSS selector to wait for before scraping
timeout: 30000                      # Milliseconds
headless: true
selectors:
  - name: "title"                   # Column name in output DataFrame
    css: "article.product_pod h3 a" # CSS selector
  - name: "price"
    css: "article.product_pod .price_color"
```

> Requires `playwright install chromium` to be run first.

### Transformers

#### `pass_through` — No-Op Transformer

Returns the DataFrame unchanged. Useful for testing.

```yaml
# No config needed
```

#### `pydantic_validation` — Pydantic Model Validation

Validates each row against a Pydantic BaseModel. Invalid rows are dropped with a warning.

```yaml
model: "data_extractor.schemas.todo.TodoItem"  # Dotted import path to a Pydantic model
chunk_size: 1000                                # Process in chunks (memory efficiency)
strict: false                                   # Pydantic strict mode
```

#### `data_cleaning` — Data Cleaning Rules

Applies up to 11 cleaning rules in a fixed order:

```yaml
drop_columns: ["unwanted_col"]          # Drop specific columns
rename_columns:                          # Rename columns
  old_name: "new_name"
lowercase_columns: true                  # Lowercase all column names
strip_whitespace: true                   # Strip leading/trailing whitespace from strings
fill_nulls:                              # Fill nulls with specific values
  name: "Unknown"
drop_nulls: ["email"]                    # Drop rows where these columns are null
drop_null_columns: true                  # Drop columns that are entirely null
deduplicate: true                        # Remove duplicate rows
deduplicate_columns: ["id"]              # Dedup based on specific columns
standardize_dates:                       # Parse and standardize date columns
  created_at: "%Y-%m-%d"
cast_types:                              # Cast column types
  id: "int64"
  completed: "bool"
```

### Loaders

#### `json_local` — Local JSON File Loader

Writes the DataFrame to a local JSON file.

```yaml
output_path: "output/result.json"
orient: "records"                  # Pandas JSON orient
indent: 2
```

#### `sql_database` — SQL Database Loader

Writes to any SQLAlchemy-supported database. Supports standard modes and upsert.

**Standard mode** (append/replace/fail):
```yaml
connection_string: "sqlite:///output/data.db"
table_name: "my_table"
if_exists: "append"                # "append", "replace", or "fail"
index: false
```

**Upsert mode** (INSERT ... ON CONFLICT DO UPDATE):
```yaml
connection_string: "sqlite:///output/data.db"
table_name: "my_table"
if_exists: "upsert"
primary_keys: ["id"]               # Columns forming the unique constraint
index: false
```

Upsert support: SQLite and PostgreSQL.

## Incremental Loading

Pipelines can track a cursor (e.g., max ID or timestamp) between runs so only new or changed data is extracted.

### How it works

1. Engine reads the `incremental` config and fetches the stored cursor from `state.json`
2. The cursor value is injected into the extractor's query parameters automatically
3. After extraction, the engine computes the new cursor: `max(df[cursor_field])`
4. The new cursor is saved **only after a successful load** — if any step fails, the state is unchanged and the next run re-fetches the same window

### Configuration

```yaml
pipeline:
  name: "my_incremental_pipeline"
  extract:
    source: "rest_api"
    config_file: "configs/sources/my_api.yaml"
  load:
    destination: "sql_database"
    config_file: "configs/loaders/my_db.yaml"
  incremental:
    cursor_field: "id"            # Column to track (max value becomes the cursor)
    cursor_param: "since_id"      # Query parameter name to inject into the extractor
    initial_value: 0              # Starting value on first run

settings:
  state_file: "state.json"        # Where cursor state is persisted
```

### Full refresh

To ignore the stored cursor and re-extract everything:

```bash
python -m data_extractor -c pipeline_config.yaml --full-refresh
```

The new cursor is still saved after a successful load, so subsequent incremental runs pick up where the full refresh left off.

## Available Modules

| Category | Key | Class | Description |
|---|---|---|---|
| Extractor | `rest_api` | `RESTAPIExtractor` | HTTP API with pagination and auth |
| Extractor | `json_file` | `JSONFileExtractor` | Local JSON files |
| Extractor | `playwright_scraper` | `PlaywrightScraperExtractor` | Headless Chromium web scraping |
| Transformer | `pass_through` | `PassThroughTransformer` | No-op (returns DataFrame unchanged) |
| Transformer | `pydantic_validation` | `PydanticValidationTransformer` | Row validation against Pydantic models |
| Transformer | `data_cleaning` | `DataCleaningTransformer` | 11 configurable cleaning rules |
| Loader | `json_local` | `JSONLocalLoader` | Write to local JSON file |
| Loader | `sql_database` | `SQLAlchemyLoader` | SQL databases with upsert support |

## Example Pipelines

### REST API to JSON

```bash
python -m data_extractor -c pipeline_config.yaml
```

Pulls 200 todos from JSONPlaceholder, validates against a Pydantic model, cleans data types, and saves to `output/todos.json`.

### REST API to SQLite

```bash
python -m data_extractor -c sql_pipeline.yaml
```

Same extraction and validation, but loads into a local SQLite database at `output/todos.db`.

### Web Scraping to JSON

```bash
playwright install chromium
python -m data_extractor -c webscrape_pipeline.yaml
```

Scrapes book titles and prices from books.toscrape.com using headless Chromium, cleans the data, and saves to JSON.

### Local JSON Validation

```bash
python -m data_extractor -c demo_user_pipeline.yaml
```

Reads intentionally broken user data from `test_data/broken_users.json`, validates against a `User` Pydantic model (drops records with invalid emails, negative IDs, etc.), and saves the clean records.

## Testing

Run the full test suite (99 tests):

```bash
python -m pytest tests/ -v
```

Tests cover:
- All extractors, transformers, and loaders in isolation
- Pydantic schema validation
- Plugin registry auto-discovery
- State manager (atomic cursor persistence)
- SQL upsert (insert, update, composite keys, edge cases)
- CLI argument parsing and flag wiring
- Full end-to-end pipeline integration (JSON, SQL, incremental, full-refresh)

## Project Structure

```
Data-Extractor/
├── src/data_extractor/
│   ├── __main__.py              # CLI entry point (argparse)
│   ├── engine.py                # Pipeline orchestrator
│   ├── registry.py              # Decorator-based plugin registry
│   ├── models.py                # Pydantic config models
│   ├── state.py                 # Incremental cursor state manager
│   ├── extractors/
│   │   ├── base.py              # BaseExtractor ABC
│   │   ├── rest_api.py          # REST API extractor (httpx)
│   │   ├── json_file.py         # Local JSON file extractor
│   │   └── playwright_scraper.py # Web scraper (Playwright)
│   ├── transformers/
│   │   ├── base.py              # BaseTransformer ABC
│   │   ├── pass_through.py      # No-op transformer
│   │   ├── pydantic_validation.py # Pydantic row validation
│   │   └── data_cleaning.py     # 11-rule data cleaning
│   ├── loaders/
│   │   ├── base.py              # BaseLoader ABC
│   │   ├── json_local.py        # JSON file loader
│   │   └── sqlalchemy_loader.py # SQL database loader (with upsert)
│   └── schemas/
│       ├── todo.py              # TodoItem Pydantic model
│       └── user.py              # User Pydantic model
├── configs/
│   ├── sources/                 # Extractor configs
│   ├── transforms/              # Transformer configs
│   └── loaders/                 # Loader configs
├── tests/                       # 99 pytest tests
├── pipeline_config.yaml         # REST API → JSON pipeline
├── sql_pipeline.yaml            # REST API → SQLite pipeline
├── webscrape_pipeline.yaml      # Web scrape → JSON pipeline
├── demo_user_pipeline.yaml      # Broken data validation demo
├── pyproject.toml               # Package config and dependencies
└── README.md
```
