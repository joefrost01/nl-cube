/**
 * NL-Cube Upload Utilities
 * Handles file uploads to subjects with progress tracking
 */

class UploadManager {
    constructor(options = {} ) {
        this.baseUrl = options.baseUrl || '/api';
        this.maxConcurrent = options.maxConcurrent || 2;
        this.chunkSize = options.chunkSize || 1024 * 1024; // 1MB chunks
        this.onProgress = options.onProgress || (() => {});
        this.onError = options.onError || (() => {});
        this.onSuccess = options.onSuccess || (() => {});
        this.onComplete = options.onComplete || (() => {});

        this.activeUploads = 0;
        this.queue = [];
        this.totalProgress = 0;
        this.fileCount = 0;
        this.completedFiles = 0;
    }

    /**
     * Add files to the upload queue for a specific subject
     * @param {string} subjectName - The subject to upload to
     * @param {FileList|Array} files - Files to upload
     */
    addToQueue(subjectName, files) {
        this.fileCount = files.length;
        this.completedFiles = 0;
        this.totalProgress = 0;

        for (let i = 0; i < files.length; i++) {
            const file = files[i];

            // Validate file type
            if (!this.isValidFileType(file)) {
                this.onError({
                    file: file.name,
                    error: 'Unsupported file type. Only CSV and Parquet are allowed.'
                });
                continue;
            }

            this.queue.push({
                file,
                subject: subjectName,
                progress: 0,
                status: 'queued'
            });
        }

        // Start upload process if not already running
        this.processQueue();
    }

    /**
     * Process the upload queue
     */
    processQueue() {
        if (this.queue.length === 0 && this.activeUploads === 0) {
            this.onComplete();
            return;
        }

        // Start uploads up to max concurrent limit
        while (this.activeUploads < this.maxConcurrent && this.queue.length > 0) {
            const uploadItem = this.queue.shift();
            uploadItem.status = 'uploading';
            this.activeUploads++;

            this.uploadFile(uploadItem).finally(() => {
                this.activeUploads--;
                this.completedFiles++;
                this.processQueue();
            });
        }
    }

    /**
     * Upload a single file
     * @param {Object} uploadItem - The item to upload
     */
    async uploadFile(uploadItem) {
        try {
            const formData = new FormData();
            formData.append('file', uploadItem.file);

            // Create XMLHttpRequest for progress tracking
            const xhr = new XMLHttpRequest();

            // Track upload progress
            xhr.upload.addEventListener('progress', (event) => {
                if (event.lengthComputable) {
                    const fileProgress = Math.round((event.loaded / event.total) * 100);
                    uploadItem.progress = fileProgress;

                    // Calculate total progress across all files
                    this.updateTotalProgress();
                }
            });

            // Return a promise for the upload
            const result = await new Promise((resolve, reject) => {
                xhr.open('POST', `${this.baseUrl}/upload/${uploadItem.subject}`);

                xhr.onload = () => {
                    if (xhr.status >= 200 && xhr.status < 300) {
                        try {
                            const response = JSON.parse(xhr.responseText);
                            uploadItem.status = 'completed';
                            uploadItem.progress = 100;
                            this.updateTotalProgress();
                            resolve(response);
                        } catch (error) {
                            uploadItem.status = 'error';
                            uploadItem.error = 'Invalid response';
                            reject(new Error('Invalid response from server'));
                        }
                    } else {
                        uploadItem.status = 'error';
                        uploadItem.error = xhr.statusText;
                        reject(new Error(`Upload failed: ${xhr.statusText}`));
                    }
                };

                xhr.onerror = () => {
                    uploadItem.status = 'error';
                    uploadItem.error = 'Network error';
                    reject(new Error('Network error during upload'));
                };

                xhr.send(formData);
            });

            // Call success callback
            this.onSuccess({
                file: uploadItem.file.name,
                response: result
            });

            return result;
        } catch (error) {
            // Call error callback
            this.onError({
                file: uploadItem.file.name,
                error: error.message
            });

            throw error;
        }
    }

    /**
     * Update the total progress across all files
     */
    updateTotalProgress() {
        let totalProgressSum = 0;

        // Calculate progress across all files in queue
        this.queue.forEach(item => {
            totalProgressSum += item.progress;
        });

        // Add progress of completed files
        totalProgressSum += this.completedFiles * 100;

        // Calculate total progress percentage
        this.totalProgress = Math.round(totalProgressSum / this.fileCount);

        // Call progress callback
        this.onProgress({
            totalProgress: this.totalProgress,
            completedFiles: this.completedFiles,
            totalFiles: this.fileCount
        });
    }

    /**
     * Check if the file type is supported
     * @param {File} file - The file to check
     * @returns {boolean} - Whether the file type is supported
     */
    isValidFileType(file) {
        const allowedTypes = [
            'text/csv',
            'application/csv',
            'application/parquet',
            'application/vnd.apache.parquet',
            // Allow files without MIME type but with correct extension
            ''
        ];

        // Check MIME type first
        const isValidMime = allowedTypes.includes(file.type);

        // Also check extension
        const extension = file.name.split('.').pop().toLowerCase();
        const isValidExtension = ['csv', 'parquet', 'pqt'].includes(extension);

        return isValidMime || isValidExtension;
    }

    /**
     * Cancel all pending uploads
     */
    cancelAll() {
        this.queue = [];
        this.onComplete();
    }
}
export default UploadManager;