// This is where the Axum start-up code goes as well as the handlers
// we'll need a post-handler for when the user drops in a file via the UI
// the post-handler will need to be multipart/streaming and stream to a temp file on disk 
// which can then be uploaded via DuckDB.

// At some point we'll likely do a Tauri version but just by bundling Axum and having the browser
// part talk via loopback to minimise the code changes.

// We'll stream arrow results directly out of DuckDB for the queries as we'll be passing that 
// to FINOS perspective in the UI and that can handle Arrow as input