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
import ReportsManager from './reports-utils.js';

// Constants
const API_BASE_URL = '/api';
const DEFAULT_THEME = 'dark';

// State
const appState = {
    currentSubject: null,
    currentQuery: null,
    subjects: [],
    queryHistory: [],
    currentTheme: localStorage.getItem('theme') || DEFAULT_THEME
};

// Make appState available globally for other components
window.appState = appState;

// Initialize managers
let perspectiveManager;
let queryManager;
let uploadManager;
let reportsManager;

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
            fetchAndUpdateReports()
        ]);

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
        },
        onQueryComplete: (queryItem, metadata) => {
            // Update UI to show completion
            const runButton = document.getElementById('runQueryBtn');
            runButton.disabled = false;
            runButton.innerHTML = '<i class="bi bi-play-fill"></i> Run Query';

            // Update current query in app state
            appState.currentQuery = queryItem;

            // Update the history UI
            updateQueryHistoryUI();

            // Refresh reports menu to enable save option
            fetchAndUpdateReports();
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

            // Show completion toast
            showToast('File upload complete', 'success');

            // Re-enable the upload button
            document.getElementById('uploadFilesSubmitBtn').disabled = false;
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

        // Initialize Reports Manager with the Perspective Manager
        reportsManager = new ReportsManager({
            baseUrl: API_BASE_URL,
            perspectiveManager: perspectiveManager,
            onReportSaved: (report) => {
                showToast(`Report "${report.name}" saved successfully`, 'success');
                // Update reports dropdown
                reportsManager.updateReportsUI(
                    document.getElementById('reportsDropdownMenu'),
                    (reportId) => loadReport(reportId),
                    () => {
                        const modal = new bootstrap.Modal(document.getElementById('saveReportModal'));
                        modal.show();
                    }
                );
            },
            onReportLoaded: (report) => {
                // Update UI when report is loaded
                document.getElementById('nlQueryInput').value = report.question || '';
                document.getElementById('generatedSqlDisplay').textContent = report.sql || '';
            },
            onError: (error) => {
                showToast(`Report operation failed: ${error.message}`, 'error');
            }
        });

        console.log('Perspective initialized');
        return true;
    } catch (error) {
        console.error('Failed to initialize Perspective:', error);
        showToast('Error initializing visualization engine', 'error');
        return false;
    }
}

// Fetch reports and update the UI
async function fetchAndUpdateReports() {
    try {
        // Fetch the reports
        const reports = await reportsManager.fetchReports();

        // Update the UI with the reports and callbacks
        reportsManager.updateReportsUI(
            document.getElementById('reportsDropdownMenu'),
            (reportId) => loadReport(reportId),
            () => {
                if (appState.currentQuery) {
                    const modal = new bootstrap.Modal(document.getElementById('saveReportModal'));
                    modal.show();
                } else {
                    showToast('Run a query before saving a report', 'error');
                }
            }
        );

        return reports;
    } catch (error) {
        console.error('Error fetching reports:', error);
        showToast('Failed to load saved reports', 'error');
        return [];
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

    // Clear history button in dropdown
    document.getElementById('clearHistoryBtn').addEventListener('click', function(e) {
        e.stopPropagation(); // Prevent dropdown from closing
        appState.queryHistory = [];
        updateQueryHistoryUI();
        showToast('Query history cleared', 'success');
    });

    // Create subject/database
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
    document.getElementById('saveReportSubmitBtn').addEventListener('click', handleSaveReport);

    // Theme toggle
    //document.getElementById('themeToggleBtn').addEventListener('click', toggleTheme);

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

    // Initialize query history UI
    updateQueryHistoryUI();
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

async function handleNlQuery(e) {
    e.preventDefault();

    const question = document.getElementById('nlQueryInput').value.trim();
    if (!question) return;

    try {
        // Update UI to show loading state
        const runButton = document.getElementById('runQueryBtn');
        runButton.disabled = true;
        runButton.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Running...';

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

        // Update query history - now in the dropdown
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

        // Reset button and enable save button
        runButton.disabled = false;
        runButton.innerHTML = '<i class="bi bi-play-fill"></i> Run Query';

        // Update reports menu to enable save option
        fetchAndUpdateReports();

    } catch (error) {
        // Handle errors
        console.error('Error executing query:', error);

        // Reset button state
        const runButton = document.getElementById('runQueryBtn');
        runButton.disabled = false;
        runButton.innerHTML = '<i class="bi bi-play-fill"></i> Run Query';

        // Show error
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
    const resultsContainer = document.querySelector('.perspective-container .card-body');
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

// Create a new database
async function handleCreateSubject() {
    const subjectName = document.getElementById('newSubjectName').value.trim();

    if (!subjectName) {
        showToast('Database name is required', 'error');
        return;
    }

    // Validate database name (alphanumeric with underscores)
    if (!/^[a-zA-Z0-9_]+$/.test(subjectName)) {
        showToast('Database name must contain only letters, numbers, and underscores', 'error');
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

        // Refresh databases list and select the new one
        await fetchSubjects();
        selectSubject(subjectName);

        showToast(`Database "${subjectName}" created successfully`, 'success');
    } catch (error) {
        console.error('Error creating subject:', error);
        showToast(`Failed to create subject: ${error.message}`, 'error');
    }
}

// Upload files to a subject
async function handleFileUpload() {
    if (!appState.currentSubject) {
        showToast('Please select a database first', 'error');
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
    try {
        // Call API to set the current subject
        const response = await fetch(`${API_BASE_URL}/subjects/select/${subjectName}`, {
            method: 'POST'
        });

        if (!response.ok) {
            throw new Error(`Failed to select subject: ${response.statusText}`);
        }

        // Update local state
        appState.currentSubject = subjectName;

        // Update UI
        const currentSubjectNameEl = document.getElementById('currentSubjectName');
        if (currentSubjectNameEl) {
            currentSubjectNameEl.textContent = subjectName;
        }

        const uploadBtn = document.getElementById('uploadFilesBtn');
        if (uploadBtn) {
            uploadBtn.disabled = false;
        }

        // Fetch subject details - now includes the tables list
        await fetchSubjectDetails(subjectName);

        showToast(`Subject "${subjectName}" selected`, 'success');
    } catch (error) {
        console.error(`Error selecting subject ${subjectName}:`, error);
        showToast(`Failed to select subject: ${error.message}`, 'error');
    }
}

// Handle table view
async function viewTable(tableName) {
    try {
        // Simplify the query to avoid schema qualification issues
        const simpleQuery = `SELECT * FROM "${tableName}" LIMIT 10000`;

        // Get a connection and query the table directly
        const response = await fetch(`${API_BASE_URL}/query`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                query: simpleQuery
            })
        });

        if (!response.ok) {
            throw new Error(`Failed to query table: ${response.statusText}`);
        }

        // Get metadata from headers
        const totalCount = parseInt(response.headers.get('x-total-count') || '0', 10);
        const executionTime = parseInt(response.headers.get('x-execution-time') || '0', 10);

        // Get the Arrow data
        const arrowBuffer = await response.arrayBuffer();

        // Load the data into perspective
        if (arrowBuffer.byteLength > 0) {
            const success = await loadArrowData(arrowBuffer);
            if (!success) {
                showToast(`Failed to load table data`, 'error');
            }
        } else {
            // Create mock data for empty tables
            let emptyTableData = {
                'Table': [tableName],
                'Status': ['Empty table (0 rows)']
            };

            await perspectiveManager.loadJsonData(emptyTableData);
        }

        // Update query history with a synthetic entry
        addToQueryHistory(
            `View table ${tableName}`,
            simpleQuery,
            executionTime,
            totalCount
        );

        // Enable save by updating reports menu
        fetchAndUpdateReports();

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
        const reportData = {
            name: name,
            category: category,
            description: description,
            question: appState.currentQuery.question,
            sql: appState.currentQuery.sql
        };

        // Use reports manager to save the report
        await reportsManager.saveReport(reportData);

        // Close modal
        bootstrap.Modal.getInstance(document.getElementById('saveReportModal')).hide();

        // Clear inputs
        document.getElementById('reportName').value = '';
        document.getElementById('reportCategory').value = '';
        document.getElementById('reportDescription').value = '';

    } catch (error) {
        console.error('Error saving report:', error);
        showToast(`Failed to save report: ${error.message}`, 'error');
    }
}

// Load a saved report
async function loadReport(reportId) {
    try {
        // Use the reports manager to load the report
        const report = await reportsManager.loadReport(reportId);

        if (!report) {
            throw new Error('Failed to load report');
        }

        // Update current query in app state
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

        // Refresh reports menu to enable save option
        fetchAndUpdateReports();

        return report;
    } catch (error) {
        console.error('Error loading report:', error);
        showToast(`Failed to load report: ${error.message}`, 'error');
        return null;
    }
}

// UI Updates

// Update databases dropdown
function updateSubjectsUI() {
    const databasesMenu = document.getElementById('subjectsDropdownMenu');
    databasesMenu.innerHTML = '';

    // Add "New" option at the top
    const newItem = document.createElement('li');
    const newLink = document.createElement('a');
    newLink.className = 'dropdown-item fw-bold';
    newLink.href = '#';
    newLink.innerHTML = '<i class="bi bi-plus-circle"></i> New Database';
    newLink.addEventListener('click', () => {
        const modal = new bootstrap.Modal(document.getElementById('createSubjectModal'));
        modal.show();
    });
    newItem.appendChild(newLink);
    databasesMenu.appendChild(newItem);

    // Add divider if there are subjects
    if (appState.subjects.length > 0) {
        const divider = document.createElement('li');
        divider.innerHTML = '<hr class="dropdown-divider">';
        databasesMenu.appendChild(divider);
    }

    // Add all existing databases
    if (appState.subjects.length === 0) {
        const item = document.createElement('li');
        item.innerHTML = '<span class="dropdown-item-text text-muted">No databases available</span>';
        databasesMenu.appendChild(item);
    } else {
        appState.subjects.forEach(subject => {
            const item = document.createElement('li');
            const link = document.createElement('a');
            link.className = 'dropdown-item';
            link.href = '#';
            link.textContent = subject;
            link.addEventListener('click', () => selectSubject(subject));
            item.appendChild(link);
            databasesMenu.appendChild(item);
        });
    }
}

// Update subject details UI - simplified to show just a list of tables
function updateSubjectDetailsUI(subjectDetails) {
    const tablesContainer = document.getElementById('tablesListContainer');

    if (!subjectDetails) {
        tablesContainer.innerHTML = '<p class="text-muted">Select a database to see tables</p>';
        return;
    }

    if (subjectDetails.tables.length === 0) {
        tablesContainer.innerHTML = '<p class="text-muted">No tables available. Upload data files to create tables.</p>';
        return;
    }

    // Create a list of tables with view buttons
    let html = '<ul class="list-group list-group-flush">';

    subjectDetails.tables.forEach(table => {
        html += `
            <li class="list-group-item d-flex justify-content-between align-items-center py-2">
                <span class="table-name">${table}</span>
                <button class="btn btn-sm btn-outline-primary btn-view-table" 
                        data-table="${table}">View</button>
            </li>
        `;
    });

    html += '</ul>';
    tablesContainer.innerHTML = html;

    // Add event listeners for view table buttons
    document.querySelectorAll('.btn-view-table').forEach(btn => {
        btn.addEventListener('click', function() {
            const tableName = this.getAttribute('data-table');
            viewTable(tableName);
        });
    });
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

    // Add to history (limit to 25 items)
    appState.queryHistory.unshift(historyItem);
    if (appState.queryHistory.length > 25) {
        appState.queryHistory.pop(); // Remove oldest
    }

    // Update UI
    updateQueryHistoryUI();
}

// Update query history UI - now for the dropdown in navbar
function updateQueryHistoryUI() {
    const historyContainer = document.getElementById('queryHistoryList');
    if (!historyContainer) return;

    const history = appState.queryHistory;

    if (history.length === 0) {
        historyContainer.innerHTML = '<li><span class="dropdown-item-text text-muted">No query history yet</span></li>';
        return;
    }

    historyContainer.innerHTML = '';

    // Display most recent queries first (up to 25)
    const displayCount = Math.min(history.length, 25);
    for (let i = 0; i < displayCount; i++) {
        const item = history[i];
        const historyItem = document.createElement('li');

        // Format timestamp
        const timestamp = new Date(item.timestamp);
        const timeString = timestamp.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

        // Create the history item
        const itemContent = document.createElement('div');
        itemContent.className = 'query-history-item';
        itemContent.innerHTML = `
            <div class="d-flex justify-content-between mb-1">
                <span class="text-primary">${item.question}</span>
                <small class="text-muted">${timeString}</small>
            </div>
            <div class="text-muted small">
                <span class="badge bg-secondary">${item.rowCount} rows</span>
                <span class="ms-2">${item.executionTime}ms</span>
            </div>
        `;

        // Add click handler to re-run the query
        itemContent.addEventListener('click', function() {
            document.getElementById('nlQueryInput').value = item.question;
            document.getElementById('generatedSqlDisplay').textContent = item.sql || '';

            // Close dropdown
            const dropdownEl = document.getElementById('historyDropdown');
            const dropdown = bootstrap.Dropdown.getInstance(dropdownEl);
            if (dropdown) dropdown.hide();

            handleNlQuery(new Event('submit'));
        });

        historyItem.appendChild(itemContent);
        historyContainer.appendChild(historyItem);
    }
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