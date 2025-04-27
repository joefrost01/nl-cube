# NL-Cube

**NL-Cube** is an **offline-first, natural language analytics engine** that transforms CSV and Parquet files into lightning-fast, pivotable data cubes powered by **DuckDB** — with full **natural language querying** built-in.

No cloud dependency. No setup hell. One binary. Your data, your control.

## Highlights

- **Natural Language Querying**  
  Ask real questions ("Top 5 regions by revenue in 2025") and get instant answers.

- **Flexible Data Ingestion**  
  Drop CSV or Parquet files into folders to populate your own subject areas.

- **Lightning-Fast Execution**  
  Queries are translated into optimized DuckDB SQL and executed instantly.

- **Embedded UI**  
  Analyze, pivot, and visualize your data using an integrated **FINOS Perspective** front-end.

- **Offline, Secure, Private**  
  Runs entirely on your laptop or server. No mandatory cloud APIs. Full local control.

- **Model Flexibility**  
  Supports multiple LLM backends (local LLMs like SQLCoder via ezllama, or cloud models via Rig).

## How It Works

1. **Create Subjects**  
   Organize your datasets into logical folders called "Subjects".

2. **Drop Data**  
   Add CSV or Parquet files. NL-Cube ingests them automatically.

3. **Ask Questions**  
   Query your data in natural language. NL-Cube translates your question into SQL.

4. **Explore Results**  
   Use the embedded Perspective UI to slice, dice, and visualize your answers.

## Key Technologies

- **Rust** for extreme performance and reliability.
- **DuckDB** for fast, embeddable analytics.
- **SQLCoder / Rig / ezllama** for local and flexible LLM-driven querying.
- **Axum + HTMX + Perspective** for the lightweight, powerful UI.

## Philosophy

NL-Cube is designed for engineers, analysts, and builders who want:

- Zero cloud lock-in.
- True local-first analytics.
- Full control over how their data is queried and presented.

If you can describe it, NL-Cube will help you analyze it.

---

> **NL-Cube**: "Your data. Your questions. Your machine."

