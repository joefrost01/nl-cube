/**
 * NL-Cube Perspective Utilities
 * Manages the Perspective viewer for data visualization
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

            // Create an empty table to start
            const emptyTable = await this.worker.table({
                message: 'string'
            });

            await emptyTable.update([{
                message: 'No data loaded. Enter a query or select a dataset.'
            }]);

            // Load the table
            await this.viewerElement.load(emptyTable);
            this.table = emptyTable;

            // Set default plugin
            this.viewerElement.setAttribute('plugin', 'datagrid');

            // Apply default configuration if provided
            if (Object.keys(this.defaultConfig).length > 0) {
                await this.viewerElement.restore(this.defaultConfig);
            }

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
     * Load Arrow data into the Perspective viewer
     * @param {ArrayBuffer} arrowData - The Arrow data to load
     * @returns {Promise} - Resolves when the data is loaded
     */
    async loadArrowData(arrowData) {
        try {
            // If there's an existing table, delete it
            if (this.table) {
                await this.table.delete();
            }

            // Create a new table from Arrow data
            this.table = await this.worker.table(arrowData);

            // Load the table into the viewer
            await this.viewerElement.load(this.table);

            return true;
        } catch (error) {
            console.error('Error loading data into Perspective:', error);
            this.onError(error);
            return false;
        }
    }

    /**
     * Load JSON data into the Perspective viewer
     * @param {Object|Array} jsonData - The JSON data to load
     * @returns {Promise} - Resolves when the data is loaded
     */
    async loadJsonData(jsonData) {
        try {
            // If there's an existing table, delete it
            if (this.table) {
                await this.table.delete();
            }

            // Create a new table from JSON data
            this.table = await this.worker.table(jsonData);

            // Load the table into the viewer
            await this.viewerElement.load(this.table);

            return true;
        } catch (error) {
            console.error('Error loading JSON data into Perspective:', error);
            this.onError(error);
            return false;
        }
    }

    /**
     * Set the visualization plugin
     * @param {string} plugin - The plugin to use (e.g., 'datagrid', 'd3_y_bar')
     */
    setPlugin(plugin) {
        if (this.viewerElement) {
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
        if (!this.viewerElement) {
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
        if (!this.viewerElement || !config) {
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
        if (!this.table) {
            throw new Error('No data to export');
        }

        try {
            return await this.table.view().to_csv();
        } catch (error) {
            console.error('Error exporting to CSV:', error);
            this.onError(error);
            throw error;
        }
    }

    /**
     * Clean up resources when done
     */
    async cleanup() {
        try {
            if (this.table) {
                await this.table.delete();
                this.table = null;
            }

            if (this.worker) {
                // No explicit way to close the worker in Perspective API
                this.worker = null;
            }
        } catch (error) {
            console.error('Error cleaning up Perspective resources:', error);
        }
    }
}

// Export the PerspectiveManager class
export default PerspectiveManager;