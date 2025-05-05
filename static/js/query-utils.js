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