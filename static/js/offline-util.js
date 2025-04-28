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

    /**
     * Register the service worker for offline support
     * @param {string} swPath - Path to the service worker file
     * @returns {Promise<boolean>} - Whether registration was successful
     */
    async registerServiceWorker(swPath) {
        if (!('serviceWorker' in navigator)) {
            console.warn('Service workers are not supported in this browser');
            return false;
        }

        try {
            const registration = await navigator.serviceWorker.register(swPath);
            console.log('Service worker registered:', registration);
            return true;
        } catch (error) {
            console.error('Service worker registration failed:', error);
            return false;
        }
    }

    /**
     * Cache resources for offline use
     * @param {Array<string>} urls - URLs to cache
     * @returns {Promise<boolean>} - Whether caching was successful
     */
    async cacheResources(urls) {
        if (!this.cacheEnabled || !('caches' in window)) {
            return false;
        }

        try {
            const cache = await caches.open(`${this.appName}-v${this.version}`);
            await cache.addAll(urls);
            return true;
        } catch (error) {
            console.error('Error caching resources:', error);
            return false;
        }
    }

    /**
     * Check if a resource is cached
     * @param {string} url - URL to check
     * @returns {Promise<boolean>} - Whether the resource is cached
     */
    async isResourceCached(url) {
        if (!this.cacheEnabled || !('caches' in window)) {
            return false;
        }

        try {
            const cache = await caches.open(`${this.appName}-v${this.version}`);
            const response = await cache.match(url);
            return !!response;
        } catch (error) {
            console.error('Error checking cached resource:', error);
            return false;
        }
    }

    /**
     * Fetch a resource with offline support
     * @param {string} url - URL to fetch
     * @param {object} options - Fetch options
     * @returns {Promise<Response>} - The response
     */
    async fetchWithFallback(url, options = {}) {
        // Try network first
        if (this.isOnline) {
            try {
                const response = await fetch(url, options);

                // If successful and we have caching enabled, update the cache
                if (response.ok && this.cacheEnabled && 'caches' in window && options.method !== 'POST') {
                    const cache = await caches.open(`${this.appName}-v${this.version}`);
                    cache.put(url, response.clone());
                }

                return response;
            } catch (error) {
                console.warn('Network fetch failed, trying cache:', error);
                // Fall through to cache
            }
        }

        // Try from cache if network failed or offline
        if (this.cacheEnabled && 'caches' in window) {
            const cache = await caches.open(`${this.appName}-v${this.version}`);
            const cachedResponse = await cache.match(url);

            if (cachedResponse) {
                return cachedResponse;
            }
        }

        // If we get here, both network and cache failed
        throw new Error('Resource not available offline and network is unreachable');
    }

    /**
     * Save data to local storage
     * @param {string} key - Storage key
     * @param {any} data - Data to store
     * @returns {boolean} - Whether storage was successful
     */
    saveToStorage(key, data) {
        try {
            const fullKey = `${this.appName}:${key}`;
            const serialized = JSON.stringify(data);
            localStorage.setItem(fullKey, serialized);
            return true;
        } catch (error) {
            console.error('Error saving to storage:', error);
            return false;
        }
    }

    /**
     * Load data from local storage
     * @param {string} key - Storage key
     * @param {any} defaultValue - Default value if not found
     * @returns {any} - The loaded data
     */
    loadFromStorage(key, defaultValue = null) {
        try {
            const fullKey = `${this.appName}:${key}`;
            const serialized = localStorage.getItem(fullKey);

            if (serialized === null) {
                return defaultValue;
            }

            return JSON.parse(serialized);
        } catch (error) {
            console.error('Error loading from storage:', error);
            return defaultValue;
        }
    }

    /**
     * Check online status
     * @returns {boolean} - Whether the app is online
     */
    checkOnlineStatus() {
        return this.isOnline;
    }
}

// Export the OfflineManager class
export default OfflineManager;