// ============================================================================
// Image Pipeline — Real-Time Dashboard
// ============================================================================

(() => {
'use strict';

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const state = {
    jobs: new Map(),         // jobId → { id, fileName, status, dims, durationMs, ... }
    workers: new Map(),      // workerId → { id, busy, jobId, fileName }
    dlqEntries: [],
    stats: { queueLen: 0, queueCap: 0, workerCount: 0, activeWorkers: 0, totalJobs: 0, completedJobs: 0, dlqJobs: 0 },
    connected: false,
    completionTimestamps: [], // for throughput calc
    selectedFile: null,
};

// ---------------------------------------------------------------------------
// DOM refs
// ---------------------------------------------------------------------------
const $ = (id) => document.getElementById(id);
const dom = {
    connectionBadge: $('connection-badge'),
    connectionDot: $('connection-dot'),
    connectionText: $('connection-text'),
    // Pipeline stages
    stageUpload: $('stage-upload'),
    stageQueue: $('stage-queue'),
    stageWorkers: $('stage-workers'),
    stageOutput: $('stage-output'),
    conn1: $('conn-1'), conn2: $('conn-2'), conn3: $('conn-3'),
    queueBadge: $('queue-badge'),
    workersBadge: $('workers-badge'),
    outputBadge: $('output-badge'),
    // Stats
    statTotal: $('stat-total'),
    statQueued: $('stat-queued'),
    statActive: $('stat-active'),
    statCompleted: $('stat-completed'),
    statDlq: $('stat-dlq'),
    statThroughput: $('stat-throughput'),
    // Upload
    dropzone: $('dropzone'),
    dropzoneContent: $('dropzone-content'),
    dropzonePreview: $('dropzone-preview'),
    fileInput: $('file-input'),
    previewImg: $('preview-img'),
    previewName: $('preview-name'),
    previewSize: $('preview-size'),
    previewClear: $('preview-clear'),
    widthInput: $('width-input'),
    heightInput: $('height-input'),
    uploadBtn: $('upload-btn'),
    // Bulk Upload
    bulkCount: $('bulk-count'),
    bulkUploadBtn: $('bulk-upload-btn'),
    // Panels
    workerGrid: $('worker-grid'),
    queueFill: $('queue-fill'),
    queueText: $('queue-text'),
    queuePct: $('queue-pct'),
    backpressureAlert: $('backpressure-alert'),
    jobList: $('job-list'),
    jobsEmpty: $('jobs-empty'),
    dlqList: $('dlq-list'),
    dlqEmpty: $('dlq-empty'),
    dlqCount: $('dlq-count'),
    toastContainer: $('toast-container'),
};

// ---------------------------------------------------------------------------
// SSE Connection
// ---------------------------------------------------------------------------
let sse = null;
let reconnectTimeout = null;

function connectSSE() {
    if (sse) { sse.close(); }
    sse = new EventSource('/events');

    sse.addEventListener('open', () => {
        setConnected(true);
        if (reconnectTimeout) { clearTimeout(reconnectTimeout); reconnectTimeout = null; }
    });

    sse.addEventListener('error', () => {
        setConnected(false);
        sse.close();
        reconnectTimeout = setTimeout(connectSSE, 3000);
    });

    // Event handlers
    sse.addEventListener('init', (e) => handlePoolStats(JSON.parse(e.data)));
    sse.addEventListener('pool_stats', (e) => handlePoolStats(JSON.parse(e.data)));
    sse.addEventListener('job_queued', (e) => handleJobQueued(JSON.parse(e.data)));
    sse.addEventListener('job_processing', (e) => handleJobProcessing(JSON.parse(e.data)));
    sse.addEventListener('job_completed', (e) => handleJobCompleted(JSON.parse(e.data)));
    sse.addEventListener('job_dead_lettered', (e) => handleJobDeadLettered(JSON.parse(e.data)));
    sse.addEventListener('backpressure', (e) => handleBackpressure(JSON.parse(e.data)));
}

function setConnected(connected) {
    state.connected = connected;
    dom.connectionBadge.className = 'connection-badge ' + (connected ? 'connected' : 'disconnected');
    dom.connectionText.textContent = connected ? 'Connected' : 'Reconnecting…';
}

// ---------------------------------------------------------------------------
// Event Handlers
// ---------------------------------------------------------------------------
function handlePoolStats(data) {
    state.stats = {
        queueLen: data.queue_len || 0,
        queueCap: data.queue_cap || 1,
        workerCount: data.worker_count || 0,
        activeWorkers: data.active_workers || 0,
        totalJobs: data.total_jobs || 0,
        completedJobs: data.completed_jobs || 0,
        dlqJobs: data.dlq_jobs || 0,
    };
    ensureWorkerCards(state.stats.workerCount);
    updateStatsDisplay();
    updateQueueGauge();
    updatePipelineFlow();
}

function handleJobQueued(data) {
    const job = {
        id: data.job_id,
        fileName: data.file_name || 'unknown',
        status: 'queued',
        targetWidth: data.target_width,
        targetHeight: data.target_height,
        durationMs: null,
        originalSize: null,
        resizedSize: null,
    };
    state.jobs.set(job.id, job);
    renderJobEntry(job);
    updatePipelineFlow();
    showToast(`Job #${job.id} queued — ${job.fileName}`, 'info');
}

function handleJobProcessing(data) {
    const job = state.jobs.get(data.job_id);
    if (job) {
        job.status = 'processing';
        updateJobEntry(job);
    }
    // Update worker card
    const wid = data.worker_id;
    state.workers.set(wid, { id: wid, busy: true, jobId: data.job_id, fileName: data.file_name });
    updateWorkerCard(wid);
    updatePipelineFlow();
}

function handleJobCompleted(data) {
    const job = state.jobs.get(data.job_id);
    if (job) {
        job.status = 'completed';
        job.durationMs = data.duration_ms;
        job.originalSize = data.original_size;
        job.resizedSize = data.resized_size;
        updateJobEntry(job);
    }
    // Free worker
    const wid = data.worker_id;
    if (state.workers.has(wid)) {
        state.workers.set(wid, { id: wid, busy: false, jobId: null, fileName: null });
        updateWorkerCard(wid);
    }
    // Throughput tracking
    state.completionTimestamps.push(Date.now());
    state.completionTimestamps = state.completionTimestamps.filter(t => Date.now() - t < 10000);
    updatePipelineFlow();
    showToast(`Job #${data.job_id} completed in ${data.duration_ms}ms`, 'success');
}

function handleJobDeadLettered(data) {
    const job = state.jobs.get(data.job_id);
    if (job) {
        job.status = 'dead_lettered';
        updateJobEntry(job);
    }
    // Free worker
    const wid = data.worker_id;
    if (wid !== undefined && state.workers.has(wid)) {
        state.workers.set(wid, { id: wid, busy: false, jobId: null, fileName: null });
        updateWorkerCard(wid);
    }
    // Add DLQ entry
    const dlqEntry = { jobId: data.job_id, fileName: data.file_name, attempts: data.attempts, error: data.error || 'unknown' };
    state.dlqEntries.push(dlqEntry);
    renderDLQEntry(dlqEntry);
    updatePipelineFlow();
    showToast(`Job #${data.job_id} dead-lettered after ${data.attempts} attempts`, 'error');
}

function handleBackpressure(data) {
    dom.backpressureAlert.hidden = false;
    setTimeout(() => { dom.backpressureAlert.hidden = true; }, 5000);
    showToast('Backpressure! Queue full — requests rejected', 'warning');
}

// ---------------------------------------------------------------------------
// UI Updates
// ---------------------------------------------------------------------------
function updateStatsDisplay() {
    dom.statTotal.textContent = state.stats.totalJobs;
    dom.statQueued.textContent = state.stats.queueLen;
    dom.statActive.textContent = state.stats.activeWorkers;
    dom.statCompleted.textContent = state.stats.completedJobs;
    dom.statDlq.textContent = state.stats.dlqJobs;

    // Throughput: completions in last 10s, divided by 10
    const recentCompletions = state.completionTimestamps.filter(t => Date.now() - t < 10000).length;
    const throughput = recentCompletions / 10;
    dom.statThroughput.textContent = throughput.toFixed(1);
}

function updateQueueGauge() {
    const { queueLen, queueCap } = state.stats;
    const pct = queueCap > 0 ? (queueLen / queueCap) * 100 : 0;
    dom.queueFill.style.width = pct + '%';
    dom.queueText.textContent = `${queueLen} / ${queueCap}`;
    dom.queuePct.textContent = Math.round(pct) + '%';

    dom.queueFill.classList.remove('high', 'critical');
    if (pct >= 90) dom.queueFill.classList.add('critical');
    else if (pct >= 60) dom.queueFill.classList.add('high');
}

function updatePipelineFlow() {
    const { queueLen, activeWorkers, completedJobs } = state.stats;
    const hasQueued = queueLen > 0;
    const hasActive = activeWorkers > 0;

    toggleActive(dom.stageQueue, hasQueued);
    toggleActive(dom.stageWorkers, hasActive);
    toggleActive(dom.conn1, hasQueued || hasActive);
    toggleActive(dom.conn2, hasQueued || hasActive);
    toggleActive(dom.conn3, hasActive);

    setBadge(dom.queueBadge, queueLen);
    setBadge(dom.workersBadge, activeWorkers);
    setBadge(dom.outputBadge, completedJobs);
}

function toggleActive(el, active) {
    if (!el) return;
    el.classList.toggle('active', active);
}

function setBadge(el, value) {
    if (!el) return;
    el.textContent = value;
    el.classList.toggle('visible', value > 0);
}

// ---------------------------------------------------------------------------
// Worker Cards
// ---------------------------------------------------------------------------
function ensureWorkerCards(count) {
    if (dom.workerGrid.children.length === count) return;
    dom.workerGrid.innerHTML = '';
    for (let i = 0; i < count; i++) {
        const card = document.createElement('div');
        card.className = 'worker-card idle';
        card.id = `worker-${i}`;
        card.innerHTML = `
            <div class="worker-id">W-${i}</div>
            <div class="worker-status">💤</div>
            <div class="worker-status-text">Idle</div>
            <div class="worker-job">&nbsp;</div>
        `;
        dom.workerGrid.appendChild(card);
        state.workers.set(i, { id: i, busy: false, jobId: null, fileName: null });
    }
}

function updateWorkerCard(workerId) {
    const w = state.workers.get(workerId);
    const card = document.getElementById(`worker-${workerId}`);
    if (!w || !card) return;

    if (w.busy) {
        card.className = 'worker-card busy';
        card.querySelector('.worker-status').textContent = '⚡';
        card.querySelector('.worker-status-text').textContent = 'Processing';
        card.querySelector('.worker-job').textContent = `#${w.jobId} ${w.fileName || ''}`;
    } else {
        card.className = 'worker-card idle';
        card.querySelector('.worker-status').textContent = '💤';
        card.querySelector('.worker-status-text').textContent = 'Idle';
        card.querySelector('.worker-job').innerHTML = '&nbsp;';
    }
}

// ---------------------------------------------------------------------------
// Job List
// ---------------------------------------------------------------------------
function renderJobEntry(job) {
    if (dom.jobsEmpty) dom.jobsEmpty.remove();
    const existing = document.getElementById(`job-${job.id}`);
    if (existing) { updateJobEntry(job); return; }

    const el = document.createElement('div');
    el.className = 'job-entry';
    el.id = `job-${job.id}`;
    el.innerHTML = buildJobHTML(job);

    // Insert at top
    dom.jobList.prepend(el);

    // Limit visible entries
    while (dom.jobList.children.length > 50) {
        dom.jobList.lastChild.remove();
    }
}

function updateJobEntry(job) {
    const el = document.getElementById(`job-${job.id}`);
    if (!el) { renderJobEntry(job); return; }
    el.innerHTML = buildJobHTML(job);
}

function buildJobHTML(job) {
    const statusClass = `badge-${job.status}`;
    const statusLabel = {
        queued: 'Queued',
        processing: 'Processing',
        completed: 'Completed',
        dead_lettered: 'Dead Letter',
    }[job.status] || job.status;

    let durationStr = '';
    if (job.durationMs !== null) {
        durationStr = job.durationMs < 1000 ? `${job.durationMs}ms` : `${(job.durationMs / 1000).toFixed(1)}s`;
    }

    let sizeStr = '';
    if (job.originalSize && job.resizedSize) {
        sizeStr = `${formatBytes(job.originalSize)}→${formatBytes(job.resizedSize)}`;
    }

    return `
        <span class="job-id">#${job.id}</span>
        <span class="job-file" title="${escapeHtml(job.fileName)}">${escapeHtml(job.fileName)}</span>
        <span class="job-dims">${job.targetWidth || '?'}×${job.targetHeight || '?'}</span>
        <span class="job-status-badge ${statusClass}">${statusLabel}</span>
        <span class="job-duration">${durationStr || sizeStr || ''}</span>
    `;
}

// ---------------------------------------------------------------------------
// DLQ List
// ---------------------------------------------------------------------------
function renderDLQEntry(entry) {
    if (dom.dlqEmpty) dom.dlqEmpty.remove();
    dom.dlqCount.textContent = state.dlqEntries.length;
    dom.dlqCount.hidden = false;

    const el = document.createElement('div');
    el.className = 'dlq-entry';
    el.innerHTML = `
        <span class="dlq-icon">⚠️</span>
        <span class="dlq-text"><strong>#${entry.jobId}</strong> ${escapeHtml(entry.fileName)} · ${entry.attempts} attempt${entry.attempts !== 1 ? 's' : ''}</span>
        <span class="dlq-error" title="${escapeHtml(entry.error)}">${escapeHtml(entry.error)}</span>
    `;
    dom.dlqList.prepend(el);
}

// ---------------------------------------------------------------------------
// Toast Notifications
// ---------------------------------------------------------------------------
function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    dom.toastContainer.appendChild(toast);
    setTimeout(() => toast.remove(), 3200);
}

// ---------------------------------------------------------------------------
// Upload Handling
// ---------------------------------------------------------------------------
function setupUpload() {
    // Dropzone click → open file picker
    dom.dropzone.addEventListener('click', (e) => {
        if (e.target === dom.previewClear || e.target.closest('.preview-clear')) return;
        dom.fileInput.click();
    });

    // File selected via picker
    dom.fileInput.addEventListener('change', () => {
        if (dom.fileInput.files.length > 0) selectFile(dom.fileInput.files[0]);
    });

    // Drag and drop
    dom.dropzone.addEventListener('dragover', (e) => { e.preventDefault(); dom.dropzone.classList.add('drag-over'); });
    dom.dropzone.addEventListener('dragleave', () => { dom.dropzone.classList.remove('drag-over'); });
    dom.dropzone.addEventListener('drop', (e) => {
        e.preventDefault();
        dom.dropzone.classList.remove('drag-over');
        if (e.dataTransfer.files.length > 0) selectFile(e.dataTransfer.files[0]);
    });

    // Clear preview
    dom.previewClear.addEventListener('click', (e) => {
        e.stopPropagation();
        clearFile();
    });

    // Upload button
    dom.uploadBtn.addEventListener('click', doUpload);

    // Enable button when file + dimensions are valid
    dom.widthInput.addEventListener('input', validateUploadForm);
    dom.heightInput.addEventListener('input', validateUploadForm);
}

function selectFile(file) {
    if (!file.type.startsWith('image/')) {
        showToast('Please select an image file', 'warning');
        return;
    }
    state.selectedFile = file;
    dom.previewName.textContent = file.name;
    dom.previewSize.textContent = formatBytes(file.size);

    const reader = new FileReader();
    reader.onload = (e) => { dom.previewImg.src = e.target.result; };
    reader.readAsDataURL(file);

    dom.dropzoneContent.hidden = true;
    dom.dropzonePreview.hidden = false;
    validateUploadForm();
}

function clearFile() {
    state.selectedFile = null;
    dom.fileInput.value = '';
    dom.dropzoneContent.hidden = false;
    dom.dropzonePreview.hidden = true;
    dom.previewImg.src = '';
    validateUploadForm();
}

function validateUploadForm() {
    const hasFile = state.selectedFile !== null;
    const w = parseInt(dom.widthInput.value) || 0;
    const h = parseInt(dom.heightInput.value) || 0;
    dom.uploadBtn.disabled = !hasFile || (w === 0 && h === 0);
}

async function doUpload() {
    if (!state.selectedFile) return;
    const width = dom.widthInput.value || '0';
    const height = dom.heightInput.value || '0';

    if (parseInt(width) === 0 && parseInt(height) === 0) {
        showToast('Enter at least one dimension (width or height)', 'warning');
        return;
    }

    dom.uploadBtn.classList.add('loading');
    dom.uploadBtn.disabled = true;

    const formData = new FormData();
    formData.append('image', state.selectedFile);
    formData.append('width', width);
    formData.append('height', height);

    try {
        const resp = await fetch('/upload', { method: 'POST', body: formData });
        const body = await resp.json().catch(() => ({}));

        if (resp.status === 202) {
            showToast(`Job #${body.job_id} accepted`, 'success');
            clearFile();
            dom.widthInput.value = '';
            dom.heightInput.value = '';
        } else if (resp.status === 429) {
            showToast('Server at capacity — try again later', 'warning');
        } else if (resp.status === 503) {
            showToast('Server is shutting down', 'error');
        } else {
            showToast(`Upload failed: ${body.message || resp.statusText}`, 'error');
        }
} catch (err) {
        showToast(`Network error: ${err.message}`, 'error');
    } finally {
        dom.uploadBtn.classList.remove('loading');
        validateUploadForm();
    }
}

// ---------------------------------------------------------------------------
// Bulk Upload (Load Test)
// ---------------------------------------------------------------------------
function setupBulkUpload() {
    dom.bulkUploadBtn.addEventListener('click', doBulkUpload);
}

// Generates a mock JPEG image using Canvas
function generateMockImage(index) {
    return new Promise(resolve => {
        const canvas = document.createElement('canvas');
        canvas.width = 800;
        canvas.height = 600;
        const ctx = canvas.getContext('2d');
        
        // Random background color
        ctx.fillStyle = `hsl(${Math.random() * 360}, 70%, 50%)`;
        ctx.fillRect(0, 0, 800, 600);
        
        // Text
        ctx.fillStyle = '#ffffff';
        ctx.font = 'bold 48px sans-serif';
        ctx.textAlign = 'center';
        ctx.fillText(`Load Test Image #${index}`, 400, 300);

        canvas.toBlob(blob => {
            // Need a File object to mock the upload
            const file = new File([blob], `load-test-${index}.jpg`, { type: 'image/jpeg' });
            resolve(file);
        }, 'image/jpeg', 0.8);
    });
}

async function doBulkUpload() {
    const count = parseInt(dom.bulkCount.value) || 50;
    if (count <= 0) return;

    dom.bulkUploadBtn.classList.add('loading');
    dom.bulkUploadBtn.disabled = true;
    showToast(`Starting load test with ${count} images...`, 'info');

    try {
        const promises = [];
        for (let i = 1; i <= count; i++) {
            // Random target dims
            const targetW = Math.floor(Math.random() * (400 - 100) + 100);
            
            promises.push(
                generateMockImage(i).then(file => {
                    const formData = new FormData();
                    formData.append('image', file);
                    formData.append('width', targetW);
                    
                    return fetch('/upload', { method: 'POST', body: formData });
                }).catch(() => null)
            );
        }

        // Fire all concurrently
        const results = await Promise.allSettled(promises);
        
        // Count statuses
        let accepted = 0;
        let rejected = 0;
        let errors = 0;
        
        results.forEach(res => {
            if (res.status === 'fulfilled' && res.value) {
                if (res.value.status === 202) accepted++;
                else if (res.value.status === 429) rejected++;
                else errors++;
            } else {
                errors++;
            }
        });

        showToast(`Load test sent! Accepted: ${accepted}, Backpressure: ${rejected}, Errors: ${errors}`, 'success');
    } catch (err) {
        showToast(`Load test error: ${err.message}`, 'error');
    } finally {
        dom.bulkUploadBtn.classList.remove('loading');
        dom.bulkUploadBtn.disabled = false;
    }
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
function formatBytes(bytes) {
    if (bytes === 0 || bytes == null) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0) + ' ' + units[i];
}

function escapeHtml(str) {
    if (!str) return '';
    const d = document.createElement('div');
    d.textContent = str;
    return d.innerHTML;
}

// ---------------------------------------------------------------------------
// Throughput ticker — recalculate every second
// ---------------------------------------------------------------------------
setInterval(() => {
    state.completionTimestamps = state.completionTimestamps.filter(t => Date.now() - t < 10000);
    const throughput = state.completionTimestamps.length / 10;
    dom.statThroughput.textContent = throughput.toFixed(1);
}, 1000);

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
setupUpload();
setupBulkUpload();
connectSSE();

})();
