/**
 * NL-Cube Offline Utilities
 * Handles offline functionality and caching
 */

class OfflineManager {
    constructor(options = {}) {
        this.cacheEnabled = options.cacheEnabled !== false;
        this.onlineStatusChanged = options.onlineStatusChanged || (() => {});
        this.appName = options.appName || 'nl-cube';
        this.version = options.version || '1.0.0';
        this.isOnline = navigator.onLine;

        // Initialize listeners
        this._initListeners();
    }

    /**
     * Initialize event listeners for online/offline events
     * @private
     */
    _initListeners() {
        window.addEventListener('online', () => {
            this.isOnline = true;
            this.onlineStatusChanged(true);
        });

        window.addEventListener('offline', () => {
            this.isOnline = false;
            this.onlineStatusChanged(false);
        });
    }
}

// Export the OfflineManager class
export default OfflineManager;