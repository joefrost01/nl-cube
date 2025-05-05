/**
 * NL-Cube Service Worker Registration
 * Register the service worker for offline support
 */

// Check if service workers are supported
if ('serviceWorker' in navigator) {
    window.addEventListener('load', function() {
        // Register the service worker
        navigator.serviceWorker.register('/service-worker.js')
            .then(function(registration) {
                console.log('ServiceWorker registration successful with scope: ',
                    registration.scope);

                // Check for updates to the service worker
                registration.addEventListener('updatefound', function() {
                    // If there's an update, get the new service worker
                    const newWorker = registration.installing;

                    newWorker.addEventListener('statechange', function() {
                        if (newWorker.state === 'installed' && navigator.serviceWorker.controller) {
                            // New service worker available - show update notification
                            showUpdateNotification();
                        }
                    });
                });
            })
            .catch(function(error) {
                console.error('ServiceWorker registration failed: ', error);
            });
    });

    // Add a listener for the 'controllerchange' event to reload the page
    // when a new service worker takes over
    navigator.serviceWorker.addEventListener('controllerchange', function() {
        console.log('New service worker controller, reloading page for updates...');
        window.location.reload();
    });
}

/**
 * Show a notification that an update is available
 */
function showUpdateNotification() {
    // Check if we have permission to show notifications
    if (Notification.permission === 'granted') {
        navigator.serviceWorker.ready.then(function(registration) {
            registration.showNotification('NL-Cube Update Available', {
                body: 'Refresh the page to apply the latest updates.',
                icon: '/images/logo.png',
                requireInteraction: true
            });
        });
    }

    // Also show an in-app notification
    // First check if the toast container exists
    let toastContainer = document.getElementById('toastContainer');
    if (!toastContainer) {
        toastContainer = document.createElement('div');
        toastContainer.id = 'toastContainer';
        toastContainer.className = 'toast-container position-fixed bottom-0 end-0 p-3';
        document.body.appendChild(toastContainer);
    }

    // Create the update toast
    const toastId = `toast-update-${Date.now()}`;
    const toastEl = document.createElement('div');
    toastEl.id = toastId;
    toastEl.className = 'toast align-items-center text-bg-info border-0';
    toastEl.role = 'alert';
    toastEl.setAttribute('aria-live', 'assertive');
    toastEl.setAttribute('aria-atomic', 'true');

    toastEl.innerHTML = `
        <div class="d-flex">
            <div class="toast-body">
                <strong>Update Available</strong>
                <div>Refresh the page to apply the latest updates.</div>
            </div>
            <div class="d-flex align-items-center me-2">
                <button type="button" class="btn btn-sm btn-light me-2" id="update-now-btn">
                    Update Now
                </button>
                <button type="button" class="btn-close btn-close-white" 
                    data-bs-dismiss="toast" aria-label="Close"></button>
            </div>
        </div>
    `;

    // Add toast to container
    toastContainer.appendChild(toastEl);

    // Initialize and show toast
    const toast = new bootstrap.Toast(toastEl, {
        autohide: false
    });
    toast.show();

    // Add event listener to update button
    document.getElementById('update-now-btn').addEventListener('click', function() {
        window.location.reload();
    });
}

/**
 * Check for application updates
 */
function checkForAppUpdates() {
    if ('serviceWorker' in navigator) {
        navigator.serviceWorker.ready.then(function(registration) {
            registration.update()
                .then(() => console.log('Service worker update check completed'))
                .catch(error => console.error('Service worker update check failed:', error));
        });
    }
}

// Periodically check for updates (every 1 hour)
setInterval(checkForAppUpdates, 60 * 60 * 1000);

// Export functions
export { checkForAppUpdates };