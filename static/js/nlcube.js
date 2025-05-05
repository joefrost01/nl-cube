/**
 * NL-Cube Main Application
 * An offline-first, natural language analytics engine
 */

// Dynamic imports for Perspective
import "https://cdn.jsdelivr.net/npm/@finos/perspective-viewer@2.5.0/dist/cdn/perspective-viewer.js";
import "https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-datagrid@2.5.0/dist/cdn/perspective-viewer-datagrid.js";
import "https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-d3fc@2.5.0/dist/cdn/perspective-viewer-d3fc.js";
import perspective from "https://cdn.jsdelivr.net/npm/@finos/perspective@2.5.0/dist/cdn/perspective.js";

// Import utility modules
import UploadManager from './upload-utils.js';
import QueryManager from './query-utils.js';
import PerspectiveManager from './perspective-utils.js';

// Constants
const API_BASE_URL = '/api';
const DEFAULT_THEME = 'dark';

// State
const appState = {
    currentSubject: null,
    currentQuery: null,
    subjects: [],
    reports: [],
    queryHistory: [],  // Added explicit queryHistory array
    currentTheme: localStorage.getItem('theme') || DEFAULT_THEME
};

// Initialize Perspective viewer
async function initPerspectiveViewer() {
    try {
        const viewer = document.getElementById('perspectiveViewer');
        if (!viewer) {
            console.error('Perspective viewer element not found');
            return false;
        }

        // Create empty table to start
        const emptyTable = await window.perspective.worker().table({
            message: ['No data loaded. Enter a query or select a dataset.']
        });

        // Load the empty table
        await viewer.load(emptyTable);

        // Store reference for cleanup
        window.perspectiveTable = emptyTable;

        // Set theme based on current app theme
        viewer.setAttribute('theme', appState.currentTheme === 'dark' ? 'Pro Dark' : 'Pro Light');

        // Set default plugin
        viewer.setAttribute('plugin', 'datagrid');

        console.log('Perspective viewer initialized');
        return true;
    } catch (error) {
        console.error('Error initializing Perspective viewer:', error);
        return false;
    }
}

// Initialize managers
let perspectiveManager;
let queryManager;
let uploadManager;

// Initialize app when DOM is ready
document.addEventListener('DOMContentLoaded', initApp);

async function initApp() {
    console.log('Initializing NL-Cube...');

    try {
        // Initialize managers
        initManagers();

        // Initialize Perspective
        await initPerspective();

        // Setup event listeners
        setupEventListeners();

        // Initialize UI components
        initUI();

        // Load initial data
        await Promise.all([
            fetchSubjects(),
            fetchSchema(),
            fetchReports()
        ]);

        // Apply saved theme
        applyTheme(appState.currentTheme);

        console.log('NL-Cube initialized successfully');
    } catch (error) {
        console.error('Initialization failed:', error);
        showToast('Failed to initialize application', 'error');
    }
}

// Initialize utility managers
function initManagers() {
    // Initialize Query Manager
    queryManager = new QueryManager({
        baseUrl: API_BASE_URL,
        onQueryStart: (question, sql) => {
            // Update UI to show loading state
            const runButton = document.getElementById('runQueryBtn');
            runButton.disabled = true;
            runButton.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Running...';

            // Show active query in UI
            document.getElementById('resultsTitle').textContent = 'Running Query...';
        },
        onQueryComplete: (queryItem, metadata) => {
            // Update UI to show completion
            const runButton = document.getElementById('runQueryBtn');
            runButton.disabled = false;
            runButton.innerHTML = '<i class="bi bi-play-fill"></i> Run Query';

            // Update results title
            document.getElementById('resultsTitle').textContent = 'Query Results';

            // Enable save report button
            document.getElementById('saveReportBtn').disabled = false;

            // Update current query in app state
            appState.currentQuery = queryItem;

            // Update the history UI
            updateQueryHistoryUI();
        },
        onSqlGenerated: (sql) => {
            // Update SQL display
            document.getElementById('generatedSqlDisplay').textContent = sql;
        },
        onError: (error) => {
            // Update UI to show error
            const runButton = document.getElementById('runQueryBtn');
            runButton.disabled = false;
            runButton.innerHTML = '<i class="bi bi-play-fill"></i> Run Query';

            // Show error toast
            showToast(`Query failed: ${error.message}`, 'error');
        }
    });

    // Initialize Upload Manager
    uploadManager = new UploadManager({
        baseUrl: API_BASE_URL,
        onProgress: (progressInfo) => {
            // Update progress bar
            const progressBar = document.getElementById('uploadProgress');
            progressBar.classList.remove('d-none');
            const progressBarInner = progressBar.querySelector('.progress-bar');
            progressBarInner.style.width = `${progressInfo.totalProgress}%`;
            progressBarInner.setAttribute('aria-valuenow', progressInfo.totalProgress);
        },
        onSuccess: (fileInfo) => {
            // Show success message
            const statusContainer = document.getElementById('uploadStatusMessages');
            const fileElement = document.createElement('div');
            fileElement.className = 'alert alert-success';
            fileElement.textContent = `Successfully uploaded ${fileInfo.file}`;
            statusContainer.appendChild(fileElement);
        },
        onError: (errorInfo) => {
            // Show error message
            const statusContainer = document.getElementById('uploadStatusMessages');
            const fileElement = document.createElement('div');
            fileElement.className = 'alert alert-danger';
            fileElement.textContent = `Failed to upload ${errorInfo.file}: ${errorInfo.error}`;
            statusContainer.appendChild(fileElement);
        },
        onComplete: () => {
            // Refresh data after uploads complete
            fetchSubjectDetails(appState.currentSubject);
            fetchSchema();

            // Show completion toast
            showToast('File upload complete', 'success');
        }
    });
}

// Initialize Perspective viewer
async function initPerspective() {
    try {
        console.log('Initializing Perspective...');

        // Dynamically import Perspective modules
        const perspectiveModule = await import('https://cdn.jsdelivr.net/npm/@finos/perspective@2.5.0/dist/cdn/perspective.js');
        await import('https://cdn.jsdelivr.net/npm/@finos/perspective-viewer@2.5.0/dist/cdn/perspective-viewer.js');
        await import('https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-datagrid@2.5.0/dist/cdn/perspective-viewer-datagrid.js');
        await import('https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-d3fc@2.5.0/dist/cdn/perspective-viewer-d3fc.js');

        // Store the module for later use
        window.perspectiveModule = perspectiveModule.default;

        // Initialize Perspective Manager
        perspectiveManager = new PerspectiveManager({
            viewerElement: document.getElementById('perspectiveViewer'),
            theme: appState.currentTheme,
            onViewerReady: (viewer) => {
                console.log('Perspective viewer ready');
            },
            onError: (error) => {
                console.error('Perspective error:', error);
                showToast('Error in visualization engine', 'error');
            }
        });

        // Initialize the manager with the perspective module
        await perspectiveManager.initialize(perspectiveModule.default);

        console.log('Perspective initialized');
        return true;
    } catch (error) {
        console.error('Failed to initialize Perspective:', error);
        showToast('Error initializing visualization engine', 'error');
        return false;
    }
}

// Setup event listeners for all interactive elements
function setupEventListeners() {
    // NL Query form
    document.getElementById('nlQueryForm').addEventListener('submit', handleNlQuery);

    // Show SQL toggle
    document.getElementById('showSqlToggle').addEventListener('change', function(e) {
        const sqlCollapse = document.getElementById('sqlPreviewCollapse');
        if (e.target.checked) {
            new bootstrap.Collapse(sqlCollapse, { show: true });
        } else {
            new bootstrap.Collapse(sqlCollapse, { hide: true });
        }
    });

    // Create subject
    document.getElementById('createSubjectBtn').addEventListener('click', function() {
        const modal = new bootstrap.Modal(document.getElementById('createSubjectModal'));
        modal.show();
    });
    document.getElementById('createSubjectSubmitBtn').addEventListener('click', handleCreateSubject);

    // Upload files
    document.getElementById('uploadFilesBtn').addEventListener('click', function() {
        if (!appState.currentSubject) return;

        // Reset upload form
        document.getElementById('fileUploadInput').value = '';
        document.getElementById('uploadStatusMessages').innerHTML = '';
        document.getElementById('uploadProgress').classList.add('d-none');

        // Set subject name in modal
        document.getElementById('uploadSubjectName').textContent = appState.currentSubject;

        // Show modal
        const modal = new bootstrap.Modal(document.getElementById('uploadFilesModal'));
        modal.show();
    });
    document.getElementById('uploadFilesSubmitBtn').addEventListener('click', handleFileUpload);

    // Save report
    document.getElementById('saveReportBtn').addEventListener('click', function() {
        const modal = new bootstrap.Modal(document.getElementById('saveReportModal'));
        modal.show();
    });
    document.getElementById('saveReportSubmitBtn').addEventListener('click', handleSaveReport);

    // Clear history
    document.getElementById('clearHistoryBtn').addEventListener('click', function() {
        queryManager.clearHistory();
        updateQueryHistoryUI();
    });

    // Theme toggle
    document.getElementById('themeToggleBtn').addEventListener('click', toggleTheme);

    // View type dropdown
    document.querySelectorAll('[data-view-type]').forEach(item => {
        item.addEventListener('click', function(e) {
            e.preventDefault();
            const viewType = this.getAttribute('data-view-type');
            perspectiveManager.setPlugin(viewType);
        });
    });

    // Export data
    document.getElementById('exportDataBtn').addEventListener('click', handleExportData);

    // Add event listener for Enter key in query input
    document.getElementById('nlQueryInput').addEventListener('keydown', function(e) {
        if (e.key === 'Enter' && e.ctrlKey) {
            e.preventDefault();
            document.getElementById('nlQueryForm').dispatchEvent(new Event('submit'));
        }
    });
}

// Initialize UI components
function initUI() {
    // Create tooltips
    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    [...tooltipTriggerList].map(tooltipTriggerEl => new bootstrap.Tooltip(tooltipTriggerEl));
}

// Toggle between light and dark themes
function toggleTheme() {
    const newTheme = appState.currentTheme === 'dark' ? 'light' : 'dark';
    applyTheme(newTheme);
    localStorage.setItem('theme', newTheme);
    appState.currentTheme = newTheme;
}

// Apply theme to all components
function applyTheme(theme) {
    // Apply to HTML
    document.documentElement.setAttribute('data-bs-theme', theme);

    // Apply to Perspective viewer using the manager
    if (perspectiveManager) {
        perspectiveManager.setTheme(theme);
    }

    // Update theme toggle button
    const themeBtn = document.getElementById('themeToggleBtn');
    const themeIcon = themeBtn.querySelector('i');

    if (theme === 'dark') {
        themeIcon.className = 'bi bi-sun-fill';
    } else {
        themeIcon.className = 'bi bi-moon-fill';
    }

    // Add transition class to all elements
    document.body.classList.add('theme-transition');

    // Remove transition class after transition completes
    setTimeout(() => {
        document.body.classList.remove('theme-transition');
    }, 300);
}

// API Calls
// Fetch subjects list
async function fetchSubjects() {
    try {
        const response = await fetch(`${API_BASE_URL}/subjects`);

        if (!response.ok) {
            throw new Error(`Failed to fetch subjects: ${response.statusText}`);
        }

        const subjects = await response.json();
        appState.subjects = subjects;
        updateSubjectsUI();

        return subjects;
    } catch (error) {
        console.error('Error fetching subjects:', error);
        showToast('Failed to load subjects', 'error');
        return [];
    }
}

// Fetch subject details
async function fetchSubjectDetails(subjectName) {
    try {
        const response = await fetch(`${API_BASE_URL}/subjects/${subjectName}`);

        if (!response.ok) {
            throw new Error(`Failed to fetch subject details: ${response.statusText}`);
        }

        const subjectDetails = await response.json();
        updateSubjectDetailsUI(subjectDetails);

        return subjectDetails;
    } catch (error) {
        console.error(`Error fetching details for subject ${subjectName}:`, error);
        showToast(`Failed to load details for ${subjectName}`, 'error');
        return null;
    }
}

// Fetch database schema
async function fetchSchema() {
    try {
        const response = await fetch(`${API_BASE_URL}/schema`);

        if (!response.ok) {
            throw new Error(`Failed to fetch schema: ${response.statusText}`);
        }

        const schema = await response.json();
        updateSchemaUI(schema);

        return schema;
    } catch (error) {
        console.error('Error fetching schema:', error);
        showToast('Failed to load database schema', 'error');
        return null;
    }
}

// Fetch saved reports
async function fetchReports() {
    try {
        const response = await fetch(`${API_BASE_URL}/reports`);

        if (!response.ok) {
            throw new Error(`Failed to fetch reports: ${response.statusText}`);
        }

        const reports = await response.json();
        appState.reports = reports;
        updateReportsUI();

        return reports;
    } catch (error) {
        console.error('Error fetching reports:', error);
        showToast('Failed to load saved reports', 'error');
        return [];
    }
}


/**
 * Check if Perspective is truly available and ready to use
 * @returns {boolean} - Whether Perspective is fully available
 */
function isPerspectiveAvailable() {
    // Check if the global perspective object exists
    if (typeof window.perspective === 'undefined' || !window.perspective) {
        console.warn('Perspective global object not found');
        return false;
    }

    // Check if the worker function exists
    if (typeof window.perspective.worker !== 'function') {
        console.warn('Perspective worker function not found');
        return false;
    }

    // Check if the viewer element exists
    const viewer = document.getElementById('perspectiveViewer');
    if (!viewer) {
        console.warn('Perspective viewer element not found');
        return false;
    }

    console.log('Perspective is available');
    return true;
}

async function handleNlQuery(e) {
    e.preventDefault();

    const question = document.getElementById('nlQueryInput').value.trim();
    if (!question) return;

    try {
        // Update UI to show loading state
        const runButton = document.getElementById('runQueryBtn');
        runButton.disabled = true;
        runButton.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Running...';
        document.getElementById('resultsTitle').textContent = 'Running Query...';

        // Execute the query
        const response = await fetch(`${API_BASE_URL}/nl-query`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ question })
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(errorText || `Query failed with status: ${response.status}`);
        }

        // Get metadata from headers
        const generatedSql = response.headers.get('x-generated-sql') || '';
        const totalCount = parseInt(response.headers.get('x-total-count') || '0', 10);

        // Update SQL display
        document.getElementById('generatedSqlDisplay').textContent = generatedSql;

        // Get query execution time
        const executionTime = parseInt(response.headers.get('x-execution-time') || '0', 10);

        // Update query history
        addToQueryHistory(question, generatedSql, executionTime, totalCount);

        // Update current query in app state
        appState.currentQuery = {
            question,
            sql: generatedSql,
            executionTime,
            rowCount: totalCount
        };

        // Get the Arrow data
        const arrowBuffer = await response.arrayBuffer();
        console.log('Received Arrow data:', arrowBuffer.byteLength, 'bytes');

        // Check content type to confirm it's Arrow data
        const contentType = response.headers.get('content-type');
        console.log('Content type:', contentType);

        if (contentType === 'application/vnd.apache.arrow.file' && arrowBuffer.byteLength > 0) {
            try {
                const success = await loadArrowData(arrowBuffer);
                if (success) {
                    console.log('Data loaded into Perspective successfully');

                    // Make sure the viewer is visible
                    document.getElementById('perspectiveViewer').style.display = 'block';

                    // Remove any fallback display
                    const resultsContainer = document.querySelector('.card-body[style*="height"]');
                    const existingFallback = resultsContainer.querySelector('.p-3');
                    if (existingFallback) {
                        resultsContainer.removeChild(existingFallback);
                    }
                } else {
                    console.warn('Failed to load data into Perspective, using fallback');
                    showFallbackDisplay(totalCount, generatedSql, executionTime);
                }
            } catch (error) {
                console.error('Error processing Arrow data:', error);
                showFallbackDisplay(totalCount, generatedSql, executionTime);
            }
        } else {
            console.warn('Response is not Arrow data or is empty');
            showFallbackDisplay(totalCount, generatedSql, executionTime);
        }

        // Update results title
        document.getElementById('resultsTitle').textContent = `Results: ${totalCount} rows`;

        // Reset button and enable save button
        runButton.disabled = false;
        runButton.innerHTML = '<i class="bi bi-play-fill"></i> Run Query';
        document.getElementById('saveReportBtn').disabled = false;

    } catch (error) {
        // Handle errors
        console.error('Error executing query:', error);

        // Reset button state
        const runButton = document.getElementById('runQueryBtn');
        runButton.disabled = false;
        runButton.innerHTML = '<i class="bi bi-play-fill"></i> Run Query';

        // Show error
        document.getElementById('resultsTitle').textContent = 'Query Failed';
        showToast(`Query failed: ${error.message}`, 'error');
    }
}

/**
 * Show a fallback display when Perspective visualization fails
 * @param {number} rowCount - Number of rows in result
 * @param {string} sql - SQL query
 * @param {number} executionTime - Query execution time in ms
 */
function showFallbackDisplay(rowCount, sql, executionTime) {
    // Hide the perspective viewer
    const viewer = document.getElementById('perspectiveViewer');
    if (viewer) {
        viewer.style.display = 'none';
    }

    // Create and show fallback display
    const resultsContainer = document.querySelector('.card-body[style*="height"]');
    if (!resultsContainer) return;

    // Remove any existing fallback
    const existingFallback = resultsContainer.querySelector('.p-3');
    if (existingFallback) {
        resultsContainer.removeChild(existingFallback);
    }

    // Create new fallback
    const fallbackDiv = document.createElement('div');
    fallbackDiv.className = 'p-3';
    fallbackDiv.innerHTML = `
        <div class="alert alert-success">
            <h5>Query successfully executed</h5>
            <p>Your query returned ${rowCount} rows.</p>
            <p>SQL: <code>${sql}</code></p>
            <p>Execution time: ${executionTime}ms</p>
        </div>
    `;

    resultsContainer.appendChild(fallbackDiv);
}

/**
 * Load Arrow data into Perspective using direct module reference
 * @param {ArrayBuffer} arrowBuffer - The Arrow data in IPC format
 * @returns {Promise<boolean>} - Whether loading was successful
 */
async function loadArrowData(arrowBuffer) {
    try {
        console.log('Loading Arrow data, size:', arrowBuffer.byteLength, 'bytes');

        // Make sure the data is valid
        if (!arrowBuffer || arrowBuffer.byteLength === 0) {
            console.error('Empty or invalid Arrow data buffer');
            return false;
        }

        // Use dynamic import to ensure we have the module
        console.log('Importing Perspective module directly');
        const perspectiveModule = await import('https://cdn.jsdelivr.net/npm/@finos/perspective@2.5.0/dist/cdn/perspective.js');

        // Create a worker from the imported module
        const worker = perspectiveModule.default.worker();

        // Clean up existing table
        if (window.perspectiveTable) {
            try {
                await window.perspectiveTable.delete();
                window.perspectiveTable = null;
            } catch (e) {
                console.warn('Error cleaning up previous table:', e);
            }
        }

        // Get the viewer element
        const viewer = document.getElementById('perspectiveViewer');
        if (!viewer) {
            console.error('Perspective viewer element not found');
            return false;
        }

        // Create table from Arrow buffer
        console.log('Creating table from Arrow buffer');
        const table = await worker.table(arrowBuffer);

        // Load the table into the viewer
        console.log('Loading table into viewer');
        await viewer.load(table);

        // Store reference for later
        window.perspectiveTable = table;

        console.log('Arrow data loaded successfully into Perspective');
        return true;
    } catch (error) {
        console.error('Error loading Arrow data:', error);
        return false;
    }
}

// Create a new subject
async function handleCreateSubject() {
    const subjectName = document.getElementById('newSubjectName').value.trim();

    if (!subjectName) {
        showToast('Subject name is required', 'error');
        return;
    }

    // Validate subject name (alphanumeric with underscores)
    if (!/^[a-zA-Z0-9_]+$/.test(subjectName)) {
        showToast('Subject name must contain only letters, numbers, and underscores', 'error');
        return;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/subjects/${subjectName}`, {
            method: 'POST'
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(errorText || `Failed to create subject: ${response.statusText}`);
        }

        // Close modal
        bootstrap.Modal.getInstance(document.getElementById('createSubjectModal')).hide();

        // Clear input
        document.getElementById('newSubjectName').value = '';

        // Refresh subjects list and select the new one
        await fetchSubjects();
        selectSubject(subjectName);

        showToast(`Subject "${subjectName}" created successfully`, 'success');
    } catch (error) {
        console.error('Error creating subject:', error);
        showToast(`Failed to create subject: ${error.message}`, 'error');
    }
}

// Upload files to a subject
async function handleFileUpload() {
    if (!appState.currentSubject) {
        showToast('Please select a subject first', 'error');
        return;
    }

    const fileInput = document.getElementById('fileUploadInput');
    if (fileInput.files.length === 0) {
        showToast('Please select at least one file to upload', 'error');
        return;
    }

    // Clear previous status messages
    const statusContainer = document.getElementById('uploadStatusMessages');
    statusContainer.innerHTML = '';

    try {
        // Disable upload button during process
        document.getElementById('uploadFilesSubmitBtn').disabled = true;

        // Use the upload manager to upload files
        uploadManager.addToQueue(appState.currentSubject, fileInput.files);
    } catch (error) {
        console.error('Upload error:', error);

        statusContainer.innerHTML = `
            <div class="alert alert-danger">
                Upload failed: ${error.message}
            </div>
        `;

        // Hide progress bar and re-enable button
        document.getElementById('uploadProgress').classList.add('d-none');
        document.getElementById('uploadFilesSubmitBtn').disabled = false;
    }
}

// Handle subject selection
async function selectSubject(subjectName) {
    appState.currentSubject = subjectName;

    // Update UI
    document.getElementById('currentSubjectName').textContent = subjectName;
    document.getElementById('uploadFilesBtn').disabled = false;

    // Fetch subject details
    await fetchSubjectDetails(subjectName);
}

// Handle table view
async function viewTable(tableName) {
    try {
        // Update results title
        document.getElementById('resultsTitle').textContent = `Table: ${tableName}`;

        // Get a connection and query the table directly
        const response = await fetch(`${API_BASE_URL}/query`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                query: `SELECT * FROM ${appState.currentSubject}.${tableName} LIMIT 10000`
            })
        });

        if (!response.ok) {
            throw new Error(`Failed to query table: ${response.statusText}`);
        }

        // Get response data
        const data = await response.json();

        // Create mock data based on the response
        let mockData = {};

        // Create at least one row of sample data
        if (data.columns && data.columns.length > 0) {
            for (const column of data.columns) {
                mockData[column] = ["Sample data"];
            }
        } else {
            mockData = {
                'Table': [tableName],
                'Rows': [data.row_count + " rows"]
            };
        }

        // Load the mock data
        await perspectiveManager.loadJsonData(mockData);

        // Enable save button
        document.getElementById('saveReportBtn').disabled = false;

    } catch (error) {
        console.error(`Error viewing table ${tableName}:`, error);
        showToast(`Failed to load table: ${error.message}`, 'error');
    }
}

// Handle save report
async function handleSaveReport() {
    if (!appState.currentQuery) {
        showToast('Run a query before saving a report', 'error');
        return;
    }

    const name = document.getElementById('reportName').value.trim();
    const category = document.getElementById('reportCategory').value.trim();
    const description = document.getElementById('reportDescription').value.trim();

    if (!name || !category) {
        showToast('Name and category are required', 'error');
        return;
    }

    try {
        // Get current Perspective viewer configuration
        const viewerConfig = await perspectiveManager.saveConfig();

        const reportData = {
            name: name,
            category: category,
            description: description,
            question: appState.currentQuery.question,
            sql: appState.currentQuery.sql,
            config: viewerConfig
        };

        const response = await fetch(`${API_BASE_URL}/reports`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(reportData)
        });

        if (!response.ok) {
            throw new Error(`Failed to save report: ${response.statusText}`);
        }

        // Close modal
        bootstrap.Modal.getInstance(document.getElementById('saveReportModal')).hide();

        // Clear inputs
        document.getElementById('reportName').value = '';
        document.getElementById('reportCategory').value = '';
        document.getElementById('reportDescription').value = '';

        // Refresh reports list
        await fetchReports();

        showToast(`Report "${name}" saved successfully`, 'success');
    } catch (error) {
        console.error('Error saving report:', error);
        showToast(`Failed to save report: ${error.message}`, 'error');
    }
}

// Handle export data
async function handleExportData() {
    try {
        // Export using Perspective manager
        const csv = await perspectiveManager.exportToCsv();

        // Create a download link
        const blob = new Blob([csv], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `nlcube-export-${new Date().toISOString().slice(0, 10)}.csv`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);

        showToast('Data exported successfully', 'success');
    } catch (error) {
        console.error('Error exporting data:', error);
        showToast(`Export failed: ${error.message}`, 'error');
    }
}

// UI Updates

// Update subjects dropdown
function updateSubjectsUI() {
    const subjectsMenu = document.getElementById('subjectsDropdownMenu');
    subjectsMenu.innerHTML = '';

    if (appState.subjects.length === 0) {
        const item = document.createElement('li');
        item.innerHTML = '<span class="dropdown-item-text">No subjects available</span>';
        subjectsMenu.appendChild(item);
    } else {
        appState.subjects.forEach(subject => {
            const item = document.createElement('li');
            const link = document.createElement('a');
            link.className = 'dropdown-item';
            link.href = '#';
            link.textContent = subject;
            link.addEventListener('click', () => selectSubject(subject));
            item.appendChild(link);
            subjectsMenu.appendChild(item);
        });
    }
}

// Update subject details
function updateSubjectDetailsUI(subjectDetails) {
    const detailsContainer = document.getElementById('subjectDetailContent');

    if (!subjectDetails) {
        detailsContainer.innerHTML = '<p class="text-muted">Select a subject to see details</p>';
        return;
    }

    let html = `
        <div class="d-flex justify-content-between mb-2">
            <span><strong>Files:</strong> ${subjectDetails.file_count}</span>
            <span><strong>Tables:</strong> ${subjectDetails.tables.length}</span>
        </div>
    `;

    if (subjectDetails.tables.length > 0) {
        html += '<div class="mt-2"><strong>Available Tables:</strong></div>';
        html += '<ul class="table-list mt-1">';

        subjectDetails.tables.forEach(table => {
            html += `
                <li class="table-list-item">
                    <span>${table}</span>
                    <button class="btn btn-sm btn-outline-primary btn-view-table" 
                            data-table="${table}">View</button>
                </li>
            `;
        });

        html += '</ul>';
    } else {
        html += '<p class="text-muted mt-2">No tables available. Upload data files to create tables.</p>';
    }

    detailsContainer.innerHTML = html;

    // Add event listeners for view table buttons
    document.querySelectorAll('.btn-view-table').forEach(btn => {
        btn.addEventListener('click', function() {
            const tableName = this.getAttribute('data-table');
            viewTable(tableName);
        });
    });
}

// Update schema display
function updateSchemaUI(schema) {
    const schemaContainer = document.getElementById('schemaViewerContent');

    if (!schema) {
        schemaContainer.innerHTML = '<p class="text-muted">No schema available</p>';
        return;
    }

    // Parse the schema SQL to a more readable format
    const tables = parseSchemaSQL(schema);

    if (tables.length === 0) {
        schemaContainer.innerHTML = '<p class="text-muted">No tables in schema</p>';
        return;
    }

    let html = '';

    // Add collapsible sections for each table
    tables.forEach((table, index) => {
        const tableId = `schema-table-${index}`;
        const isFirstTable = index === 0;

        html += `
        <div class="accordion-item schema-table mb-2">
            <h2 class="accordion-header" id="heading-${tableId}">
                <button class="accordion-button ${isFirstTable ? '' : 'collapsed'}" type="button" 
                        data-bs-toggle="collapse" data-bs-target="#collapse-${tableId}" 
                        aria-expanded="${isFirstTable ? 'true' : 'false'}" aria-controls="collapse-${tableId}">
                    <span class="schema-table-name">${table.name}</span>
                    <span class="ms-2 badge bg-secondary">${table.columns.length} columns</span>
                </button>
            </h2>
            <div id="collapse-${tableId}" class="accordion-collapse collapse ${isFirstTable ? 'show' : ''}" 
                 aria-labelledby="heading-${tableId}">
                <div class="accordion-body p-0">
                    <table class="table table-sm mb-0">
                        <thead>
                            <tr>
                                <th>Column</th>
                                <th>Type</th>
                                <th>Nullable</th>
                            </tr>
                        </thead>
                        <tbody>`;

        table.columns.forEach(column => {
            html += `
                <tr>
                    <td class="schema-column-name">${column.name}</td>
                    <td class="schema-column-type">${column.type}</td>
                    <td>${column.nullable ? 'Yes' : 'No'}</td>
                </tr>`;
        });

        html += `
                        </tbody>
                    </table>
                </div>
            </div>
        </div>`;
    });

    schemaContainer.innerHTML = html;
}

// Update reports dropdown
function updateReportsUI() {
    const reportsMenu = document.getElementById('reportsDropdownMenu');
    reportsMenu.innerHTML = '';

    if (appState.reports.length === 0) {
        const item = document.createElement('li');
        item.innerHTML = '<span class="dropdown-item-text">No saved reports</span>';
        reportsMenu.appendChild(item);
        return;
    }

    // Group reports by category
    const reportsByCategory = {};

    appState.reports.forEach(report => {
        const category = report.category || 'Uncategorized';

        if (!reportsByCategory[category]) {
            reportsByCategory[category] = [];
        }

        reportsByCategory[category].push(report);
    });

    // Add to dropdown
    for (const category in reportsByCategory) {
        // Add category header
        const header = document.createElement('li');
        header.innerHTML = `<h6 class="dropdown-header">${category}</h6>`;
        reportsMenu.appendChild(header);

        // Add reports in this category
        reportsByCategory[category].forEach(report => {
            const item = document.createElement('li');
            const link = document.createElement('a');
            link.className = 'dropdown-item';
            link.href = '#';
            link.textContent = report.name;
            link.addEventListener('click', () => loadReport(report.id));
            item.appendChild(link);
            reportsMenu.appendChild(item);
        });

        // Add divider
        const divider = document.createElement('li');
        divider.innerHTML = '<hr class="dropdown-divider">';
        reportsMenu.appendChild(divider);
    }

    // Remove last divider
    if (reportsMenu.lastChild && reportsMenu.lastChild.querySelector('hr')) {
        reportsMenu.removeChild(reportsMenu.lastChild);
    }
}

// Load a saved report
async function loadReport(reportId) {
    try {
        const response = await fetch(`${API_BASE_URL}/reports/${reportId}`);

        if (!response.ok) {
            throw new Error(`Failed to load report: ${response.statusText}`);
        }

        const report = await response.json();

        // Update UI
        document.getElementById('nlQueryInput').value = report.question || '';
        document.getElementById('generatedSqlDisplay').textContent = report.sql || '';
        document.getElementById('resultsTitle').textContent = `Results: ${report.name}`;

        // Update the current query details
        appState.currentQuery = {
            question: report.question || 'Loaded from saved report',
            sql: report.sql
        };

        // Create mock data based on the report
        const mockData = {
            'Report Name': [report.name],
            'Category': [report.category],
            'Query': [report.question || 'N/A'],
            'SQL': [report.sql]
        };

        // Load the mock data
        await perspectiveManager.loadJsonData(mockData);

        // Apply saved view configuration if available
        if (report.config) {
            await perspectiveManager.restoreConfig(report.config);
        }

        return report;
    } catch (error) {
        console.error('Error loading report:', error);
        showToast(`Failed to load report: ${error.message}`, 'error');
        return null;
    }
}

// Add query to history
function addToQueryHistory(question, sql, executionTime, rowCount) {
    const historyItem = {
        question,
        sql,
        executionTime,
        rowCount,
        timestamp: new Date().toISOString()
    };

    // Add to history (limit to 20 items)
    appState.queryHistory.unshift(historyItem);
    if (appState.queryHistory.length > 20) {
        appState.queryHistory.pop(); // Remove oldest
    }

    // Update UI
    updateQueryHistoryUI();
}

// Update query history UI
function updateQueryHistoryUI() {
    const historyContainer = document.getElementById('queryHistoryList');
    const history = appState.queryHistory;

    if (history.length === 0) {
        historyContainer.innerHTML = '<div class="text-muted text-center py-3">No query history yet</div>';
        return;
    }

    historyContainer.innerHTML = '';

    // Display most recent queries first
    history.forEach((item, index) => {
        const historyItem = document.createElement('div');
        historyItem.className = 'query-history-item list-group-item-action';

        // Format timestamp
        const timestamp = new Date(item.timestamp);
        const timeString = timestamp.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

        historyItem.innerHTML = `
            <div class="d-flex justify-content-between align-items-center">
                <span class="query-text">${item.question}</span>
                <div class="text-end">
                    <span class="query-time">${item.executionTime}ms</span>
                    <small class="d-block text-muted">${timeString}</small>
                </div>
            </div>
        `;

        // Add click handler to re-run the query
        historyItem.addEventListener('click', function() {
            document.getElementById('nlQueryInput').value = item.question;
            document.getElementById('generatedSqlDisplay').textContent = item.sql || '';
            handleNlQuery(new Event('submit'));
        });

        historyContainer.appendChild(historyItem);
    });
}

// Parse schema SQL into structured format
function parseSchemaSQL(schemaSql) {
    const tables = [];
    const tableRegex = /CREATE TABLE (\w+) \(([\s\S]*?)\);/g;
    const columnRegex = /\s*(\w+)\s+([\w()]+)(\s+NOT NULL)?/g;

    let tableMatch;
    while ((tableMatch = tableRegex.exec(schemaSql)) !== null) {
        const tableName = tableMatch[1];
        const columnsText = tableMatch[2];

        const columns = [];
        let columnMatch;
        while ((columnMatch = columnRegex.exec(columnsText)) !== null) {
            columns.push({
                name: columnMatch[1],
                type: columnMatch[2],
                nullable: !columnMatch[3]
            });
        }

        tables.push({
            name: tableName,
            columns: columns
        });
    }

    return tables;
}

// Show toast notification (requires Bootstrap 5)
function showToast(message, type = 'info') {
    // Check if toast container exists, create if not
    let toastContainer = document.getElementById('toastContainer');
    if (!toastContainer) {
        toastContainer = document.createElement('div');
        toastContainer.id = 'toastContainer';
        toastContainer.className = 'toast-container position-fixed bottom-0 end-0 p-3';
        document.body.appendChild(toastContainer);
    }

    // Create toast element
    const toastId = `toast-${Date.now()}`;
    const toastEl = document.createElement('div');
    toastEl.id = toastId;
    toastEl.className = `toast align-items-center text-bg-${type === 'error' ? 'danger' : type} border-0`;
    toastEl.role = 'alert';
    toastEl.setAttribute('aria-live', 'assertive');
    toastEl.setAttribute('aria-atomic', 'true');

    // Set toast content
    toastEl.innerHTML = `
        <div class="d-flex">
            <div class="toast-body">
                ${message}
            </div>
            <button type="button" class="btn-close btn-close-white me-2 m-auto" 
                data-bs-dismiss="toast" aria-label="Close"></button>
        </div>
    `;

    // Add toast to container
    toastContainer.appendChild(toastEl);

    // Initialize and show toast
    const toast = new bootstrap.Toast(toastEl, {
        autohide: true,
        delay: 5000
    });
    toast.show();

    // Remove from DOM after hidden
    toastEl.addEventListener('hidden.bs.toast', function() {
        toastContainer.removeChild(toastEl);
    });
}