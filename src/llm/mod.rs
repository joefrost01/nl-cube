// Place where all LLM stuff goes, we'll use SQLCoder model via the ezllama crate for local
// for remote and Ollama we'll use Rig to abstract away the provider specifics
// we'll put all of our implementations behind a "generate_sql" trait function