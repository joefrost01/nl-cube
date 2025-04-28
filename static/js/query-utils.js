/**
 * NL-Cube Query Utilities
 * Handles natural language queries and SQL operations
 */

class QueryManager {
    constructor(options = {}) {
        this.baseUrl = options.baseUrl || '/api';
        this.onQueryStart = options.onQueryStart || (() => {});
        this.onQueryComplete = options.onQueryComplete || (() => {});
        this.onError = options.onError || (() => {});
        this.onSqlGenerated = options.onSqlGenerated || (() => {});

        this.history = this.loadHistoryFromStorage() || [];
    }

    /**
     * Execute a natural language query
     * @param {string} question - The natural language question
     * @returns {Promise} - The query results
     */
    async executeNlQuery(question) {
        try {
            // Call the query start callback
            this.onQueryStart(question);

            const startTime = performance.now();

            const response = await fetch(`${this.baseUrl}/nl-query`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ question })
            });

            if (!response.ok) {
                throw new Error(`Query failed: ${response.statusText}`);
            }

            const executionTime = Math.round(performance.now() - startTime);

            // Get the generated SQL from the header
            const generatedSql = response.headers.get('x-generated-sql') || '';

            // Call the SQL generated callback
            this.onSqlGenerated(generatedSql);

            // Get query metadata from JSON
            const metadata = await response.json();

            // Add to history
            const historyItem = {
                question,
                sql: generatedSql,
                executionTime,
                rowCount: metadata.row_count,
                columns: metadata.columns,
                timestamp: new Date().toISOString()
            };

            this.addToHistory(historyItem);

            // Call the query complete callback
            this.onQueryComplete(historyItem, metadata);

            return {
                success: true,
                sql: generatedSql,
                metadata,
                executionTime
            };
        } catch (error) {
            // Call error callback
            this.onError(error);

            return {
                success: false,
                error: error.message
            };
        }
    }

    /**
     * Execute a raw SQL query
     * @param {string} sql - The SQL query to execute
     * @returns {Promise} - The query results
     */
    async executeSqlQuery(sql) {
        try {
            // Call the query start callback
            this.onQueryStart(null, sql);

            const startTime = performance.now();

            const response = await fetch(`${this.baseUrl}/query`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ query: sql })
            });

            if (!response.ok) {
                throw new Error(`Query failed: ${response.statusText}`);
            }

            const executionTime = Math.round(performance.now() - startTime);

            // Get query metadata from JSON
            const metadata = await response.json();

            // Call the query complete callback
            this.onQueryComplete({
                sql,
                executionTime,
                rowCount: metadata.row_count,
                columns: metadata.columns,
                timestamp: new Date().toISOString()
            }, metadata);

            return {
                success: true,
                metadata,
                executionTime
            };
        } catch (error) {
            // Call error callback
            this.onError(error);

            return {
                success: false,
                error: error.message
            };
        }
    }

    /**
     * Get query history
     * @returns {Array} - The query history
     */
    getHistory() {
        return this.history;
    }

    /**
     * Add a query to history
     * @param {Object} queryItem - The query to add
     */
    addToHistory(queryItem) {
        // Add to the beginning to have newest first
        this.history.unshift(queryItem);

        // Limit history to 50 items
        if (this.history.length > 50) {
            this.history.pop();
        }

        // Save to localStorage
        this.saveHistoryToStorage();
    }

    /**
     * Clear query history
     */
    clearHistory() {
        this.history = [];
        this.saveHistoryToStorage();
    }

    /**
     * Load query history from localStorage
     * @returns {Array} - The query history
     */
    loadHistoryFromStorage() {
        try {
            const historyJson = localStorage.getItem('nlcube_query_history');
            return historyJson ? JSON.parse(historyJson) : [];
        } catch (e) {
            console.error('Failed to load query history from storage:', e);
            return [];
        }
    }

    /**
     * Save query history to localStorage
     */
    saveHistoryToStorage() {
        try {
            localStorage.setItem('nlcube_query_history', JSON.stringify(this.history));
        } catch (e) {
            console.error('Failed to save query history to storage:', e);
        }
    }
}

// Export the QueryManager class
export default QueryManager;