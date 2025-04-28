/**
 * NL-Cube Service Worker
 * Provides offline support and caching for the application
 */

const CACHE_NAME = 'nl-cube-v1.0.0';
const OFFLINE_PAGE = '/offline.html';

// Resources to cache on install
const PRECACHE_RESOURCES = [
    '/',
    '/index.html',
    '/css/nlcube.css',
    '/js/nlcube.js',
    '/js/upload-utils.js',
    '/js/query-utils.js',
    '/js/perspective-utils.js',
    '/js/offline-utils.js',
    '/offline.html',
    // Default Bootstrap and icons will be handled by CDN with fallback
];

// Install event
self.addEventListener('install', event => {
    console.log('[ServiceWorker] Install');

    event.waitUntil(
        caches.open(CACHE_NAME)
            .then(cache => {
                console.log('[ServiceWorker] Pre-caching offline resources');
                return cache.addAll(PRECACHE_RESOURCES);
            })
            .then(() => {
                console.log('[ServiceWorker] Installation complete');
                return self.skipWaiting();
            })
    );
});

// Activate event - clean up old caches
self.addEventListener('activate', event => {
    console.log('[ServiceWorker] Activate');

    event.waitUntil(
        caches.keys().then(cacheNames => {
            return Promise.all(
                cacheNames.filter(cacheName => {
                    return cacheName.startsWith('nl-cube-') && cacheName !== CACHE_NAME;
                }).map(cacheName => {
                    console.log('[ServiceWorker] Removing old cache', cacheName);
                    return caches.delete(cacheName);
                })
            );
        }).then(() => {
            console.log('[ServiceWorker] Activation complete');
            return self.clients.claim();
        })
    );
});

// API endpoint patterns to never cache
const NEVER_CACHE_PATTERNS = [
    /\/api\/upload\//,
    /\/api\/query/,
    /\/api\/nl-query/
];

// Fetch event - handle network requests with offline support
self.addEventListener('fetch', event => {
    // Skip non-GET requests and browser extensions
    if (event.request.method !== 'GET' ||
        !event.request.url.startsWith(self.location.origin)) {
        return;
    }

    // Skip API endpoints that should never be cached
    if (NEVER_CACHE_PATTERNS.some(pattern => pattern.test(event.request.url))) {
        return;
    }

    // For HTML navigation requests, use network-first strategy
    if (event.request.headers.get('accept').includes('text/html')) {
        event.respondWith(
            fetch(event.request)
                .then(response => {
                    // Cache the successful response
                    const responseClone = response.clone();
                    caches.open(CACHE_NAME).then(cache => {
                        cache.put(event.request, responseClone);
                    });
                    return response;
                })
                .catch(() => {
                    // On network failure, try from cache or fallback to offline page
                    return caches.match(event.request)
                        .then(cachedResponse => {
                            return cachedResponse || caches.match(OFFLINE_PAGE);
                        });
                })
        );
        return;
    }

    // For all other requests, use cache-first strategy
    event.respondWith(
        caches.match(event.request)
            .then(cachedResponse => {
                // Return from cache if available
                if (cachedResponse) {
                    return cachedResponse;
                }

                // Otherwise fetch from network
                return fetch(event.request)
                    .then(response => {
                        // Don't cache error responses
                        if (!response || response.status !== 200) {
                            return response;
                        }

                        // Cache the successful response for future use
                        const responseClone = response.clone();
                        caches.open(CACHE_NAME).then(cache => {
                            cache.put(event.request, responseClone);
                        });

                        return response;
                    })
                    .catch(error => {
                        console.error('[ServiceWorker] Fetch error:', error);

                        // For image requests, provide a default placeholder
                        if (event.request.url.match(/\.(jpg|png|gif|svg)$/)) {
                            return caches.match('/images/placeholder.png');
                        }

                        throw error;
                    });
            })
    );
});