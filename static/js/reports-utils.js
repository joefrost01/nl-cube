/**
 * NL-Cube Reports Utilities
 * Handles saving, loading, and managing saved reports
 */

class ReportsManager {
    constructor(options = {}) {
        this.baseUrl = options.baseUrl || '/api';
        this.onReportSaved = options.onReportSaved || (() => {});
        this.onReportLoaded = options.onReportLoaded || (() => {});
        this.onError = options.onError || (() => {});
        this.perspectiveManager = options.perspectiveManager;
        this.reports = [];
    }

    /**
     * Save the current query and visualization as a report
     * @param {Object} reportData - Report data to save
     * @returns {Promise<Object>} - The saved report
     */
    async saveReport(reportData) {
        try {
            // Validate required fields
            if (!reportData.name || !reportData.category) {
                throw new Error('Name and category are required');
            }

            // Add visualization config if perspective manager is available
            if (this.perspectiveManager) {
                try {
                    reportData.config = await this.perspectiveManager.saveConfig();
                } catch (e) {
                    console.warn('Could not save perspective configuration:', e);
                    reportData.config = {};
                }
            }

            // Send to API
            const response = await fetch(`${this.baseUrl}/reports`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(reportData)
            });

            if (!response.ok) {
                throw new Error(`Failed to save report: ${response.statusText}`);
            }

            const savedReport = await response.json();

            // Update local cache
            this.reports.push(savedReport);

            // Call callback
            this.onReportSaved(savedReport);

            return savedReport;
        } catch (error) {
            console.error('Error saving report:', error);
            this.onError(error);
            throw error;
        }
    }

    /**
     * Load all available reports
     * @returns {Promise<Array>} - List of reports
     */
    async fetchReports() {
        try {
            const response = await fetch(`${this.baseUrl}/reports`);

            if (!response.ok) {
                throw new Error(`Failed to fetch reports: ${response.statusText}`);
            }

            const reports = await response.json();
            this.reports = reports;
            return reports;
        } catch (error) {
            console.error('Error fetching reports:', error);
            this.onError(error);
            return [];
        }
    }

    /**
     * Load a specific report by ID
     * @param {string} reportId - The ID of the report to load
     * @returns {Promise<Object>} - The loaded report
     */
    async loadReport(reportId) {
        try {
            const response = await fetch(`${this.baseUrl}/reports/${reportId}`);

            if (!response.ok) {
                throw new Error(`Failed to load report: ${response.statusText}`);
            }

            const report = await response.json();

            // Apply saved view configuration if available and perspective manager is set
            if (report.config && this.perspectiveManager) {
                try {
                    await this.perspectiveManager.restoreConfig(report.config);
                } catch (e) {
                    console.warn('Could not restore perspective configuration:', e);
                }
            }

            // Call callback
            this.onReportLoaded(report);

            return report;
        } catch (error) {
            console.error('Error loading report:', error);
            this.onError(error);
            throw error;
        }
    }

    /**
     * Delete a report by ID
     * @param {string} reportId - The ID of the report to delete
     * @returns {Promise<boolean>} - Whether deletion was successful
     */
    async deleteReport(reportId) {
        try {
            const response = await fetch(`${this.baseUrl}/reports/${reportId}`, {
                method: 'DELETE'
            });

            if (!response.ok) {
                throw new Error(`Failed to delete report: ${response.statusText}`);
            }

            // Update local cache
            this.reports = this.reports.filter(report => report.id !== reportId);

            return true;
        } catch (error) {
            console.error('Error deleting report:', error);
            this.onError(error);
            return false;
        }
    }

    /**
     * Get reports grouped by category
     * @returns {Object} - Reports grouped by category
     */
    getReportsByCategory() {
        const reportsByCategory = {};

        this.reports.forEach(report => {
            const category = report.category || 'Uncategorized';

            if (!reportsByCategory[category]) {
                reportsByCategory[category] = [];
            }

            reportsByCategory[category].push(report);
        });

        return reportsByCategory;
    }

    /**
     * Update UI elements with reports data
     * @param {HTMLElement} reportsMenu - The dropdown menu element to update
     * @param {Function} loadReportCallback - Callback to run when a report is selected
     * @param {Function} saveReportCallback - Callback to run when save report is clicked
     */
    updateReportsUI(reportsMenu, loadReportCallback, saveReportCallback) {
        if (!reportsMenu) return;

        reportsMenu.innerHTML = '';

        // Add Save Report option at the top
        const saveItem = document.createElement('li');
        const saveLink = document.createElement('a');
        saveLink.className = 'dropdown-item fw-bold';
        saveLink.href = '#';
        saveLink.innerHTML = '<i class="bi bi-save"></i> Save Report';
        saveLink.id = 'saveReportBtn'; // Keep the same ID for compatibility

        // Disable if no current query
        if (!this.perspectiveManager || !window.appState?.currentQuery) {
            saveLink.classList.add('disabled');
            saveLink.setAttribute('aria-disabled', 'true');
        } else {
            saveLink.addEventListener('click', saveReportCallback);
        }

        saveItem.appendChild(saveLink);
        reportsMenu.appendChild(saveItem);

        // Add divider if there are reports
        if (this.reports.length > 0) {
            const divider = document.createElement('li');
            divider.innerHTML = '<hr class="dropdown-divider">';
            reportsMenu.appendChild(divider);
        }

        // Add all existing reports by category
        if (this.reports.length === 0) {
            const item = document.createElement('li');
            item.innerHTML = '<span class="dropdown-item-text text-muted">No saved reports</span>';
            reportsMenu.appendChild(item);
        } else {
            // Group reports by category
            const reportsByCategory = this.getReportsByCategory();

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
                    link.addEventListener('click', () => loadReportCallback(report.id));
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
    }

    /**
     * Process form data for saving a report
     * @param {Object} formData - Form field values
     * @param {Object} currentQuery - Current query details
     * @returns {Object} - Processed report data
     */
    prepareReportData(formData, currentQuery) {
        if (!currentQuery) {
            throw new Error('No query data available for the report');
        }

        return {
            name: formData.name,
            category: formData.category,
            description: formData.description || '',
            question: currentQuery.question || null,
            sql: currentQuery.sql || null
        };
    }
}

// Export the ReportsManager class
export default ReportsManager;