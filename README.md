[![CI Pipeline](https://github.com/joefrost01/nl-cube/actions/workflows/build.yml/badge.svg)](https://github.com/joefrost01/nl-cube/actions/workflows/build.yml)
[![Quarto Docs](https://img.shields.io/badge/docs-online-blue.svg)](https://joefrost01.github.io/nl-cube/)

# NL-Cube

**NL-Cube** is an **offline-first, natural language analytics engine** that transforms CSV and Parquet files into lightning-fast, pivotable data cubes powered by **DuckDB** — with full **natural language querying** built-in.

No cloud dependency. No setup hell. One binary. Your data, your control.

## Highlights

- **Natural Language Querying**  
  Ask natural questions like "Show me top 5 regions by revenue in 2025" and get instant answers.

- **Flexible Data Ingestion**  
  Drop CSV or Parquet files into subject folders to populate your own database domains.

- **Lightning-Fast Execution**  
  Queries are translated into optimized DuckDB SQL and executed instantly.

- **Embedded Visualization**  
  Analyze, pivot, and visualize your data using an integrated **FINOS Perspective** front-end.

- **Offline, Secure, Private**  
  Runs entirely on your laptop or server. No mandatory cloud APIs. Full local control.

- **Model Flexibility**  
  Supports multiple LLM backends (local models via Ollama, or remote API access).

## Architecture Overview

NL-Cube uses a modern architecture designed for performance and flexibility:

- **Rust Backend**: High-performance, reliable core functionality
- **DuckDB**: Lightning-fast embedded analytics database
- **Multiple LLM Support**: Ollama integration for local models, plus remote API options
- **Axum Web Framework**: Modern, async Rust web server
- **Perspective Visualization**: Interactive, high-performance data visualization

## How It Works

1. **Create Subjects**  
   Organize your datasets into logical domains called "Subjects."

2. **Drop Data**  
   Add CSV or Parquet files. NL-Cube ingests them automatically.

3. **Ask Questions**  
   Query your data in natural language. NL-Cube translates your question into SQL.

4. **Explore Results**  
   Use the embedded Perspective UI to slice, dice, and visualize your answers.

## Getting Started

### Installation

Download the latest binary for your platform:

```bash
# Example for Linux
curl -L https://github.com/your-repo/nl-cube/releases/latest/download/nl-cube-linux-x86_64 -o nl-cube
chmod +x nl-cube
```

### Configuration

Create a `config.toml` file or use command-line options:

```toml
data_dir = "data"

[database]
connection_string = "nl-cube.db"
pool_size = 5

[web]
host = "127.0.0.1"
port = 3000

[llm]
backend = "ollama"
model = "sqlcoder"
api_url = "http://localhost:11434/api/generate"
```

### Running

```bash
./nl-cube --config config.toml
```

Then open your browser to http://localhost:3000

## Usage

### Creating a New Database

1. Click "New Database" in the Databases dropdown
2. Enter a name for your database (e.g., "sales")
3. Upload CSV or Parquet files containing your data

### Querying Your Data

1. Select your database from the dropdown
2. Type a natural language question in the query box
3. Click "Run Query" or press Ctrl+Enter

Example questions:
- "What are the top 5 products by revenue?"
- "Show me monthly sales for 2024"
- "Compare average order value by region"

### Saving Reports

After running a query:
1. Click on "Save Report" in the Reports dropdown
2. Give your report a name and category
3. Access saved reports from the Reports dropdown anytime

## Development

### Prerequisites

- Rust 1.75+ (2024 edition)
- Ollama for local LLM support

### Building from Source

```bash
# Clone the repository
git clone https://github.com/your-repo/nl-cube.git
cd nl-cube

# Build in release mode
cargo build --release

# Run the application
./target/release/nl-cube
```

### Project Structure

- `src/` - Rust source code
    - `config.rs` - Configuration management
    - `db/` - DuckDB connection and schema management
    - `ingest/` - File ingestion (CSV, Parquet)
    - `llm/` - Language model integration
    - `web/` - Web server and API
- `static/` - Frontend assets
    - `js/` - JavaScript modules
    - `css/` - Styling
    - `index.html` - Main application page

## LLM Configuration

NL-Cube supports multiple LLM backends:

### Ollama (Local)

For offline, local execution:
```toml
[llm]
backend = "ollama"
model = "sqlcoder"  # or another SQL-oriented model
api_url = "http://localhost:11434/api/generate"
```

### Remote API

For remote LLM APIs:
```toml
[llm]
backend = "remote"
model = "model-name"
api_key = "your-api-key"
api_url = "https://api.provider.com/v1/generate"
```

## Philosophy

NL-Cube is designed for data analysts, engineers, and builders who want:

- Zero cloud lock-in
- True local-first analytics
- Full control over how their data is queried and presented
- Natural language interface to technical data

If you can describe it in plain language, NL-Cube will help you analyze it.

## License

NL-Cube is licensed under the [Apache 2 License](LICENSE).

---

> **NL-Cube**: "Your data. Your questions. Your machine."