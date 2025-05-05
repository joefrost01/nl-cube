/**
 * NL-Cube Perspective Utilities
 * Manages the Perspective viewer for data visualization with safer error handling
 */

class PerspectiveManager {
    constructor(options = {}) {
        this.viewerElement = options.viewerElement || document.getElementById('perspectiveViewer');
        this.worker = null;
        this.table = null;
        this.onViewerReady = options.onViewerReady || (() => {});
        this.onError = options.onError || (() => {});
        this.defaultConfig = options.defaultConfig || {};
        this.currentTheme = options.theme || 'dark';
        this.isInitialized = false;
    }

    /**
     * Initialize the Perspective viewer
     * @returns {Promise} - Resolves when the viewer is ready
     */
    async initialize(perspectiveModule) {
        try {
            if (!this.viewerElement) {
                throw new Error('Perspective viewer element not found');
            }

            // Create a worker
            this.worker = perspectiveModule.worker();

            // Set theme based on current app theme
            this.setTheme(this.currentTheme);

            // Create a simple empty object with just a message column
            const emptyData = { message: ['No data loaded. Enter a query or select a dataset.'] };

            // Create the table
            try {
                this.table = await this.worker.table(emptyData);
            } catch (e) {
                console.error('Error creating empty table:', e);
                // Fallback to a simpler approach
                this.table = await this.worker.table({ message: 'string' });
                await this.table.update([{ message: 'No data loaded. Enter a query or select a dataset.' }]);
            }

            // Load the table
            await this.viewerElement.load(this.table);

            // Set default plugin
            this.viewerElement.setAttribute('plugin', 'datagrid');

            // Apply default configuration if provided
            if (Object.keys(this.defaultConfig).length > 0) {
                await this.viewerElement.restore(this.defaultConfig);
            }

            // Mark as initialized
            this.isInitialized = true;

            // Call the ready callback
            this.onViewerReady(this.viewerElement);

            return true;
        } catch (error) {
            console.error('Failed to initialize Perspective:', error);
            this.onError(error);
            return false;
        }
    }

    /**
     * Create empty data table with simple structure
     * @returns {Promise} - Resolves when the empty table is created
     */
    async createEmptyTable() {
        try {
            // Create a simple empty data object
            const emptyData = {
                'Info': ['No data to display'],
                'Message': ['Run a query or select a dataset to view data']
            };

            // Create table from empty data
            return await this.worker.table(emptyData);
        } catch (error) {
            console.error('Error creating empty table:', error);
            // Even simpler fallback
            const fallbackData = { message: ['No data available'] };
            return await this.worker.table(fallbackData);
        }
    }

    /**
     * Load data into the Perspective viewer with safe cleanup
     * @param {Object} data - The data to load (JSON object or Arrow)
     * @param {String} dataType - 'json' or 'arrow'
     * @returns {Promise} - Resolves when the data is loaded
     */
    async loadData(data, dataType = 'json') {
        if (!this.isInitialized) {
            console.warn('Perspective not initialized, cannot load data');
            return false;
        }

        // Keep track of created resources to clean up in case of error
        let newTable = null;
        let loadSuccessful = false;

        try {
            // Step 1: Create new table before touching existing resources
            try {
                if (dataType === 'json') {
                    newTable = await this.worker.table(data);
                } else if (dataType === 'arrow') {
                    newTable = await this.worker.table(data);
                } else {
                    throw new Error(`Unsupported data type: ${dataType}`);
                }
            } catch (tableError) {
                console.error('Error creating new table:', tableError);
                // Try with fallback empty table
                newTable = await this.createEmptyTable();
            }

            // Step The table if it was created successfully
            if (!newTable) {
                throw new Error('Failed to create new table');
            }

            // Step 3: Load the new table into the viewer
            // Create a reference to the viewer element
            const viewer = this.viewerElement;

            try {
                // Wrap this in a timeout to prevent blocking the UI
                await new Promise((resolve, reject) => {
                    setTimeout(async () => {
                        try {
                            await viewer.load(newTable);
                            resolve();
                        } catch (e) {
                            reject(e);
                        }
                    }, 0);
                });

                loadSuccessful = true;
            } catch (loadError) {
                console.error('Error loading table into viewer:', loadError);
                // If we can't load the new table, we'll need to clean it up
                if (newTable) {
                    try {
                        await newTable.delete();
                    } catch (cleanupError) {
                        console.warn('Error cleaning up new table after load failure:', cleanupError);
                    }
                }
                throw loadError;
            }

            // Step 4: If we've made it here, it's safe to clean up the old table
            if (this.table && this.table !== newTable) {
                try {
                    await this.table.delete();
                } catch (deleteError) {
                    console.warn('Could not delete old table, but new table is loaded successfully:', deleteError);
                    // We don't need to throw here since the new table is already loaded
                }
            }

            // Step 5: Update the table reference
            this.table = newTable;

            return true;
        } catch (error) {
            console.error('Error in loadData:', error);
            this.onError(error);

            // If we created a new table but didn't successfully load it, clean it up
            if (newTable && !loadSuccessful) {
                try {
                    await newTable.delete();
                } catch (cleanupError) {
                    console.warn('Error during final cleanup:', cleanupError);
                }
            }

            // Recovery attempt: try to load a simple empty table
            try {
                const recoveryTable = await this.createEmptyTable();
                await this.viewerElement.load(recoveryTable);

                // Update reference if recovery successful
                if (this.table) {
                    try {
                        await this.table.delete();
                    } catch (e) {
                        // Ignore errors during recovery cleanup
                    }
                }
                this.table = recoveryTable;
            } catch (recoveryError) {
                console.error('Recovery failed:', recoveryError);
            }

            return false;
        }
    }

    /**
     * Load JSON data into the Perspective viewer
     * @param {Object|Array} jsonData - The JSON data to load
     * @returns {Promise} - Resolves when the data is loaded
     */
    async loadJsonData(jsonData) {
        return this.loadData(jsonData, 'json');
    }

    /**
     * Load Arrow data into the Perspective viewer
     * @param {ArrayBuffer} arrowData - The Arrow data to load
     * @returns {Promise} - Resolves when the data is loaded
     */
    async loadArrowData(arrowData) {
        return this.loadData(arrowData, 'arrow');
    }

    /**
     * Set the visualization plugin
     * @param {string} plugin - The plugin to use (e.g., 'datagrid', 'd3_y_bar')
     */
    setPlugin(plugin) {
        if (this.viewerElement && this.isInitialized) {
            this.viewerElement.setAttribute('plugin', plugin);
        }
    }

    /**
     * Set the theme for the Perspective viewer
     * @param {string} theme - 'dark' or 'light'
     */
    setTheme(theme) {
        this.currentTheme = theme;

        if (this.viewerElement) {
            this.viewerElement.setAttribute('theme', theme === 'dark' ? 'Pro Dark' : 'Pro Light');
        }
    }

    /**
     * Save the current view configuration
     * @returns {Promise<Object>} - The saved configuration
     */
    async saveConfig() {
        if (!this.viewerElement || !this.isInitialized) {
            return {};
        }

        try {
            return await this.viewerElement.save();
        } catch (error) {
            console.error('Error saving Perspective configuration:', error);
            this.onError(error);
            return {};
        }
    }

    /**
     * Restore a saved view configuration
     * @param {Object} config - The configuration to restore
     * @returns {Promise<boolean>} - Whether the restore was successful
     */
    async restoreConfig(config) {
        if (!this.viewerElement || !this.isInitialized || !config) {
            return false;
        }

        try {
            await this.viewerElement.restore(config);
            return true;
        } catch (error) {
            console.error('Error restoring Perspective configuration:', error);
            this.onError(error);
            return false;
        }
    }

    /**
     * Export the current view to CSV
     * @returns {Promise<string>} - The CSV data
     */
    async exportToCsv() {
        if (!this.table || !this.isInitialized) {
            throw new Error('No data to export');
        }

        try {
            // First get the view from the viewer
            const view = await this.viewerElement.getView();
            if (!view) {
                throw new Error('No view available for export');
            }

            // Export the view to CSV
            return await view.to_csv();
        } catch (error) {
            console.error('Error exporting to CSV:', error);
            this.onError(error);
            throw error;
        }
    }

    /**
     * Reset the viewer to an empty state
     * @returns {Promise<boolean>} - Whether the reset was successful
     */
    async reset() {
        try {
            // Create an empty table
            const emptyTable = await this.createEmptyTable();

            // Load it into the viewer
            await this.viewerElement.load(emptyTable);

            // Clean up old table if it exists
            if (this.table) {
                try {
                    await this.table.delete();
                } catch (e) {
                    console.warn('Error cleaning up old table during reset:', e);
                }
            }

            // Update reference
            this.table = emptyTable;

            return true;
        } catch (error) {
            console.error('Error resetting Perspective viewer:', error);
            this.onError(error);
            return false;
        }
    }

}

// Export the PerspectiveManager class
export default PerspectiveManager;