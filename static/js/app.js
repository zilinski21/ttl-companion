let items = [];
let displayItems = [];
let presets = [];
let skuOptions = [];
let skuPresetMap = new Map();
let shows = [];
let currentShowId = null; // Show ID for API calls

// User role and permissions (fetched from server)
let userRole = 'owner'; // default to owner until loaded
let userVisibleColumns = null; // null = all columns (owner), array = restricted (employee)
const AI_ENABLED_KEY = 'aiEnabled';
const AI_ONLY_BLANKS_KEY = 'aiOnlyBlanks';
let aiEnabled = localStorage.getItem(AI_ENABLED_KEY) === 'true';
let aiOnlyBlanks = localStorage.getItem(AI_ONLY_BLANKS_KEY);
aiOnlyBlanks = aiOnlyBlanks === null ? true : aiOnlyBlanks === 'true';
const AI_INCLUDE_SKU_KEY = 'aiIncludeSku';
let aiIncludeSku = localStorage.getItem(AI_INCLUDE_SKU_KEY);
aiIncludeSku = aiIncludeSku === null ? true : aiIncludeSku === 'true';
let aiResultsByItem = new Map();
let aiLastRunStats = null;
let aiRunning = false;
let aiLastRunTotal = null;
const AI_MATCH_ONLY_KEY = 'aiMatchOnly';
let aiMatchOnly = localStorage.getItem(AI_MATCH_ONLY_KEY);
aiMatchOnly = aiMatchOnly === 'true';
let aiDismissed = new Map();
let aiSortOverrides = new Map();

// Column sorting state
let sortColumn = null;
let sortDirection = 'asc'; // 'asc' or 'desc'
const naturalSort = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });
let sortClickLockUntil = 0;
const DEBUG_KEY = 'debugSortMode';
let debugEnabled = localStorage.getItem(DEBUG_KEY) === 'true';
let debugLastEvent = '';

// Column configuration
const columnConfig = [
    { id: 'row_number', label: '#', sortable: false, draggable: false },
    { id: 'item_name', label: 'Product Name', sortable: true, draggable: true, field: 'item_name' },
    { id: 'sold_timestamp', label: 'Sold Time', sortable: true, draggable: true, field: 'sold_timestamp' },
    { id: 'viewers', label: 'Viewers', sortable: true, draggable: true, field: 'viewers' },
    { id: 'order_id', label: 'Order ID', sortable: true, draggable: true, field: 'order_id' },
    { id: 'buyer', label: 'Buyer', sortable: true, draggable: true, field: 'buyer' },
    { id: 'cancelled_status', label: 'Cancelled/Failed', sortable: true, draggable: true, field: 'cancelled_status' },
    { id: 'sku', label: 'SKU', sortable: true, draggable: true, field: 'sku' },
    { id: 'notes', label: 'Notes', sortable: true, draggable: true, field: 'notes' },
    { id: 'preset', label: 'Preset', sortable: true, draggable: true, field: 'preset_name' },
    { id: 'pinned_message', label: 'Pinned Message', sortable: true, draggable: true, field: 'pinned_message' },
    { id: 'sold_price', label: 'Sold Price', sortable: true, draggable: true, field: 'sold_price' },
    { id: 'net_revenue', label: 'Net Revenue', sortable: true, draggable: true, field: 'net_revenue' },
    { id: 'cost', label: 'Cost', sortable: true, draggable: true, field: 'cost' },
    { id: 'profit', label: 'Profit', sortable: true, draggable: true, field: 'profit' },
    { id: 'image', label: 'Image', sortable: false, draggable: false }
];

// Column order (initialized from config, can be reordered)
let columnOrder = columnConfig.map(col => col.id);

// Hidden columns (stored separately from order)
let hiddenColumns = [];
const PRESET_BLANK = '__blank__';
const SKU_BLANK = '__sku_blank__';
const PRESET_GROUP_PREFIX = '__group__:';
const STATUS_CANCELLED = 'Cancelled';
const STATUS_FAILED = 'Failed';
const STATUS_PROCESSED = 'Processed';
let presetFilterSelected = null;
let presetFilterTemp = null;
let presetFilterOpen = false;
let presetFilterGroupSelected = new Set();
let presetFilterGroupTemp = null;
let presetFilterInitDone = false;
let presetFilterStickyItems = new Set();
let presetFilterBlankToggled = false;
let skuFilterSelected = null;
let skuFilterTemp = null;
let skuFilterOpen = false;
let skuFilterInitDone = false;
let skuFilterStickyItems = new Set();
let cancelledFilterSelected = null;
let cancelledFilterTemp = null;
let cancelledFilterOpen = false;
let cancelledFilterInitDone = false;
let filterStateLoaded = false;
let filterStateApplied = false;
let cancelledFilterPersisted = false;
let searchQuery = '';
const SORT_STATE_KEY = 'sortState';
const FILTER_STATE_KEY = 'filterState';

// Single source of truth for net revenue calculation
// TikTok takes commission + processing fees
function calcNetRevenue(soldPrice, presetName) {
    if (isGiveawayPreset(presetName)) return 0;
    if (soldPrice === null || soldPrice === undefined) return null;
    const commissionRate = parseFloat(document.getElementById('commission-rate').value) / 100;
    return soldPrice * (1 - commissionRate - 0.029) - 0.30;
}

function calcProfit(soldPrice, cost, presetName) {
    const netRevenue = calcNetRevenue(soldPrice, presetName);
    if (netRevenue === null) return null;
    return netRevenue - (parseNumber(cost) || 0);
}

// ── Toast notification system ──
function showToast(message, type = 'info', duration = 3000) {
    const container = document.getElementById('toast-container');
    if (!container) return;
    const toast = document.createElement('div');
    const colors = {
        info: 'bg-gray-800 text-white',
        success: 'bg-green-600 text-white',
        error: 'bg-red-600 text-white',
        warning: 'bg-yellow-500 text-gray-900',
        undo: 'bg-gray-800 text-white'
    };
    toast.className = `pointer-events-auto px-4 py-2.5 rounded-lg shadow-lg text-sm font-medium flex items-center gap-3 transform transition-all duration-300 translate-x-full ${colors[type] || colors.info}`;
    toast.innerHTML = `<span>${message}</span>`;
    if (type === 'undo') {
        const undoBtn = document.createElement('button');
        undoBtn.className = 'underline font-bold ml-1 hover:text-blue-300';
        undoBtn.textContent = 'Undo';
        undoBtn.onclick = () => { performUndo(); toast.remove(); };
        toast.appendChild(undoBtn);
    }
    container.appendChild(toast);
    requestAnimationFrame(() => {
        toast.classList.remove('translate-x-full');
        toast.classList.add('translate-x-0');
    });
    setTimeout(() => {
        toast.classList.add('translate-x-full');
        toast.classList.remove('translate-x-0');
        setTimeout(() => toast.remove(), 300);
    }, duration);
}

function saveSortState() {
    localStorage.setItem(
        SORT_STATE_KEY,
        JSON.stringify({ column: sortColumn, direction: sortDirection })
    );
}

function loadSortState() {
    const saved = localStorage.getItem(SORT_STATE_KEY);
    if (!saved) return;
    try {
        const data = JSON.parse(saved);
        const valid = columnConfig.find(col => col.id === data.column && col.sortable);
        if (valid) {
            sortColumn = data.column;
            sortDirection = data.direction === 'desc' ? 'desc' : 'asc';
        }
    } catch (e) {
        console.error('Error loading sort state:', e);
    }
}

function saveFilterState() {
    localStorage.setItem(
        FILTER_STATE_KEY,
        JSON.stringify({
            presetSelected: presetFilterSelected ? Array.from(presetFilterSelected) : null,
            presetGroups: Array.from(presetFilterGroupSelected || []),
            cancelledSelected: cancelledFilterSelected ? Array.from(cancelledFilterSelected) : null,
            skuSelected: skuFilterSelected ? Array.from(skuFilterSelected) : null
        })
    );
}

function loadFilterState() {
    const saved = localStorage.getItem(FILTER_STATE_KEY);
    if (!saved) {
        filterStateLoaded = true;
        return;
    }
    try {
        const data = JSON.parse(saved);
        if (data && Array.isArray(data.presetSelected)) {
            presetFilterSelected = new Set(data.presetSelected);
        } else {
            presetFilterSelected = null;
        }
        if (data && Array.isArray(data.presetGroups)) {
            presetFilterGroupSelected = new Set(data.presetGroups.map(String));
        }
        if (data && ('cancelledSelected' in data)) {
            cancelledFilterPersisted = true;
        }
        if (data && Array.isArray(data.cancelledSelected)) {
            cancelledFilterSelected = new Set(data.cancelledSelected);
        } else if (data && data.cancelledSelected === null) {
            cancelledFilterSelected = null;
        }
        if (data && Array.isArray(data.skuSelected)) {
            skuFilterSelected = new Set(data.skuSelected);
        } else if (data && data.skuSelected === null) {
            skuFilterSelected = null;
        }
    } catch (e) {
        console.error('Error loading filter state:', e);
    } finally {
        filterStateLoaded = true;
    }
}

function applySavedFilterState() {
    if (!filterStateLoaded || filterStateApplied) return;
    filterStateApplied = true;

    const optionValues = getPresetOptionList().map(opt => opt.value);
    const optionSet = new Set(optionValues);
    if (presetFilterSelected) {
        const filtered = Array.from(presetFilterSelected).filter(val => optionSet.has(val));
        if (filtered.length === 0 || filtered.length === optionValues.length) {
            presetFilterSelected = null;
        } else {
            presetFilterSelected = new Set(filtered);
        }
    }

    const groupIds = new Set(getPresetGroupOptions().map(opt => String(opt.groupId)));
    presetFilterGroupSelected = new Set(
        Array.from(presetFilterGroupSelected || []).filter(id => groupIds.has(String(id)))
    );

    if (cancelledFilterSelected) {
        const allowed = new Set([STATUS_PROCESSED, STATUS_CANCELLED, STATUS_FAILED]);
        const filtered = Array.from(cancelledFilterSelected).filter(val => allowed.has(val));
        if (filtered.length === 0 || filtered.length === allowed.size) {
            cancelledFilterSelected = null;
        } else {
            cancelledFilterSelected = new Set(filtered);
        }
    }

    const skuOptions = getSkuOptionList().map(opt => opt.value);
    if (skuFilterSelected) {
        const optionSet = new Set(skuOptions);
        const filtered = Array.from(skuFilterSelected).filter(val => optionSet.has(val));
        if (filtered.length === 0 || filtered.length === skuOptions.length) {
            skuFilterSelected = null;
        } else {
            skuFilterSelected = new Set(filtered);
        }
    }

    updatePresetFilterCount();
    updateCancelledFilterCount();
    updateSkuFilterCount();
}

// Load hidden columns from localStorage
function loadHiddenColumns() {
    const saved = localStorage.getItem('hiddenColumns');
    if (saved) {
        try {
            hiddenColumns = JSON.parse(saved).map(id => id === 'sale_price' ? 'sold_price' : id);
            // Remove any columns that no longer exist
            const configIds = columnConfig.map(col => col.id);
            hiddenColumns = hiddenColumns.filter(id => configIds.includes(id));
        } catch (e) {
            console.error('Error loading hidden columns:', e);
            hiddenColumns = [];
        }
    }
}

// Save hidden columns to localStorage
function saveHiddenColumns() {
    localStorage.setItem('hiddenColumns', JSON.stringify(hiddenColumns));
}

// Get visible columns (columnOrder minus hidden columns)
function getVisibleColumns() {
    return columnOrder.filter(colId => !hiddenColumns.includes(colId));
}

// Hide a column
function hideColumn(colId) {
    if (!hiddenColumns.includes(colId)) {
        hiddenColumns.push(colId);
        saveHiddenColumns();
        renderTable();
    }
}

// Show a column (unhide)
function showColumn(colId) {
    if (colId && hiddenColumns.includes(colId)) {
        hiddenColumns = hiddenColumns.filter(id => id !== colId);
        saveHiddenColumns();
        renderTable();
    }
}

// View modes: "sheet" (default) and "stream" (product name + image only)
let viewMode = 'sheet';

const SHEET_COLUMNS = ['row_number', 'item_name', 'order_id', 'buyer', 'cancelled_status', 'notes', 'preset', 'sold_price', 'net_revenue', 'cost', 'profit', 'image'];
const STREAM_COLUMNS = ['row_number', 'item_name', 'image'];

function toggleStreamingMode() {
    const btn = document.getElementById('streaming-mode-btn');
    viewMode = viewMode === 'sheet' ? 'stream' : 'sheet';

    const visibleCols = viewMode === 'stream' ? STREAM_COLUMNS : SHEET_COLUMNS;
    hiddenColumns = columnConfig
        .map(col => col.id)
        .filter(id => !visibleCols.includes(id));

    if (viewMode === 'stream') {
        btn.textContent = 'Sheet View';
        btn.classList.remove('bg-purple-600', 'hover:bg-purple-700');
        btn.classList.add('bg-green-600', 'hover:bg-green-700');
    } else {
        btn.textContent = 'Stream View';
        btn.classList.remove('bg-green-600', 'hover:bg-green-700');
        btn.classList.add('bg-purple-600', 'hover:bg-purple-700');
    }
    saveHiddenColumns();
    renderTable();
    updateColumnVisibilityMenu();
}

// Update the column visibility panel with checkboxes
function updateColumnVisibilityMenu() {
    const list = document.getElementById('column-visibility-list');
    if (!list) return;
    
    list.innerHTML = '';
    
    // Create checkbox list for all columns (except row_number)
    columnConfig.forEach(config => {
        if (config.id === 'row_number') return; // Skip row number

        // If employee, skip columns they aren't allowed to see
        const restrictedByRole = userVisibleColumns && !userVisibleColumns.includes(config.id);
        if (restrictedByRole) return;

        const isVisible = !hiddenColumns.includes(config.id);
        const listItem = document.createElement('div');
        listItem.style.cssText = 'display: flex; align-items: center; padding: 8px 0; border-bottom: 1px solid var(--border-color); cursor: move;';
        listItem.setAttribute('draggable', 'true');
        listItem.setAttribute('data-col-id', config.id);

        // Drag handle (dots)
        const dragHandle = document.createElement('div');
        dragHandle.innerHTML = '⋮⋮';
        dragHandle.style.cssText = 'margin-right: 10px; color: var(--text-secondary); font-size: 12px; cursor: move; user-select: none;';
        dragHandle.setAttribute('draggable', 'false');

        // Checkbox
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.checked = isVisible;
        checkbox.style.cssText = 'margin-right: 10px; cursor: pointer; width: 18px; height: 18px;';
        checkbox.addEventListener('change', function() {
            if (this.checked) {
                showColumn(config.id);
            } else {
                hideColumn(config.id);
            }
        });

        // Label
        const label = document.createElement('label');
        label.textContent = config.label;
        label.style.cssText = 'flex: 1; cursor: pointer; user-select: none; color: var(--text-color);';
        label.addEventListener('click', function(e) {
            e.preventDefault();
            checkbox.click();
        });

        listItem.appendChild(dragHandle);
        listItem.appendChild(checkbox);
        listItem.appendChild(label);
        list.appendChild(listItem);
    });
}

// Load column order from localStorage
function loadColumnOrder() {
    const saved = localStorage.getItem('columnOrder');
    if (saved) {
        try {
            let savedOrder = JSON.parse(saved);
            // Migrate old column IDs
            savedOrder = savedOrder.map(id => id === 'sale_price' ? 'sold_price' : id);
            
            const configIds = columnConfig.map(col => col.id);
            const savedIds = new Set(savedOrder);
            
            // Append any new columns that weren't previously saved
            configIds.forEach(id => {
                if (!savedIds.has(id)) {
                    savedOrder.push(id);
                }
            });
            
            // Filter out any columns no longer in config
            savedOrder = savedOrder.filter(id => configIds.includes(id));
            
            columnOrder = savedOrder;
        } catch (e) {
            console.error('Error loading column order:', e);
        }
    }
}

// Save column order to localStorage
function saveColumnOrder() {
    localStorage.setItem('columnOrder', JSON.stringify(columnOrder));
}

// Load column widths from localStorage
function loadColumnWidths() {
    const saved = localStorage.getItem('columnWidths');
    if (saved) {
        try {
            const savedWidths = JSON.parse(saved);
            if (savedWidths.sale_price && !savedWidths.sold_price) {
                savedWidths.sold_price = savedWidths.sale_price;
                delete savedWidths.sale_price;
            }
            Object.assign(columnWidths, savedWidths);
        } catch (e) {
            console.error('Error loading column widths:', e);
        }
    }
}

// Save column widths to localStorage
function saveColumnWidths() {
    localStorage.setItem('columnWidths', JSON.stringify(columnWidths));
}

// Load shows
async function loadShows() {
    try {
        const res = await fetch('/api/shows');
        shows = await res.json();
        
        const showSelect = document.getElementById('show-select');
        if (showSelect) {
            showSelect.innerHTML = '<option value="">-- Select Show --</option>' + 
                shows.map(show => 
                    `<option value="${show.id}">${escapeHtml(show.name)} - ${escapeHtml(show.date)}</option>`
                ).join('');
            
            const savedShowId = parseInt(localStorage.getItem('selectedShowId') || '', 10);
            const savedShowExists = savedShowId && shows.some(show => show.id === savedShowId);
            
            // Load current show (prefer saved selection)
            const currentShowRes = await fetch('/api/current-show');
            const currentShow = await currentShowRes.json();
            if (savedShowExists) {
                currentShowId = savedShowId;
                showSelect.value = savedShowId;
            } else if (currentShow && currentShow.id) {
                currentShowId = currentShow.id;
                showSelect.value = currentShow.id;
            } else if (shows.length > 0) {
                currentShowId = shows[0].id;
                showSelect.value = shows[0].id;
            }
            
            // Add change handler
            showSelect.addEventListener('change', async function() {
                currentShowId = parseInt(this.value) || null;
                if (currentShowId) {
                    localStorage.setItem('selectedShowId', String(currentShowId));
                } else {
                    localStorage.removeItem('selectedShowId');
                }
                if (currentShowId) {
                    await loadData();
                } else {
                    items = [];
                    renderTable();
                }
            });
        }
    } catch (error) {
        console.error('Error loading shows:', error);
    }
}

// Create new show
async function createNewShow() {
    const name = prompt('Enter show name:');
    if (!name) return;
    
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const date = `${year}-${month}-${day}`;
    
    try {
        const res = await fetch('/api/shows', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({name, date})
        });
        
        if (res.ok) {
            const newShow = await res.json();
            await loadShows();
            currentShowId = newShow.id;
            const showSelect = document.getElementById('show-select');
            if (showSelect) {
                showSelect.value = newShow.id;
            }
            await loadData(); // Reload items (should be empty now)
        } else {
            const error = await res.json();
            alert(error.error || 'Error creating show');
        }
    } catch (error) {
        alert('Error creating show: ' + error.message);
    }
}

async function duplicateShow() {
    if (!currentShowId) {
        alert('Please select a show to duplicate.');
        return;
    }
    
    if (!confirm('Duplicate the selected show? This will copy the CSV and items.')) {
        return;
    }
    
    try {
        const res = await fetch(`/api/shows/${currentShowId}/duplicate`, {
            method: 'POST'
        });
        
        if (res.ok) {
            const newShow = await res.json();
            await loadShows();
            currentShowId = newShow.id;
            const showSelect = document.getElementById('show-select');
            if (showSelect) {
                showSelect.value = newShow.id;
            }
            await loadData();
        } else {
            const error = await res.json();
            alert(error.error || 'Error duplicating show');
        }
    } catch (error) {
        alert('Error duplicating show: ' + error.message);
    }
}

async function deleteShow() {
    if (!currentShowId) {
        alert('Please select a show to delete.');
        return;
    }
    
    const showSelect = document.getElementById('show-select');
    const selectedText = showSelect && showSelect.options[showSelect.selectedIndex]
        ? showSelect.options[showSelect.selectedIndex].text
        : 'this show';
    
    if (!confirm(`Delete ${selectedText}? This will remove the show and its data.`)) {
        return;
    }
    
    try {
        const res = await fetch(`/api/shows/${currentShowId}`, {
            method: 'DELETE'
        });
        
        if (res.ok) {
            await loadShows();
            if (!currentShowId) {
                items = [];
                renderTable();
            } else {
                await loadData();
            }
        } else {
            const error = await res.json();
            alert(error.error || 'Error deleting show');
        }
    } catch (error) {
        alert('Error deleting show: ' + error.message);
    }
}

async function renameShow() {
    if (!currentShowId) {
        alert('Please select a show first');
        return;
    }
    const showSelect = document.getElementById('show-select');
    const currentText = showSelect && showSelect.options[showSelect.selectedIndex]
        ? showSelect.options[showSelect.selectedIndex].text
        : '';
    const currentName = currentText.split(' - ')[0] || '';
    const newName = prompt('Enter new show name:', currentName.trim());
    if (!newName) return;

    try {
        const res = await fetch(`/api/shows/${currentShowId}`, {
            method: 'PATCH',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ name: newName })
        });
        const result = await res.json();
        if (!res.ok) {
            alert(result.error || 'Error renaming show');
            return;
        }
        await loadShows();
        const select = document.getElementById('show-select');
        if (select) {
            select.value = currentShowId;
        }
        await loadData();
    } catch (error) {
        alert('Error renaming show: ' + error.message);
    }
}

// Recording status
let isRecording = false;
let recordingStatusInterval = null;
let dataRefreshInterval = null;
const SCROLL_POS_KEY = 'dashboardScrollY';
let scrollSaveTimer = null;
let lastUserInteractionAt = 0;
const USER_IDLE_REFRESH_MS = 4000;
let lastLoadedShowId = null;
let lastSelectionSnapshot = null;
let lastItemsFingerprint = null;
const RECORDING_REFRESH_MS = 4000;

// Check recording status
async function checkRecordingStatus() {
    try {
        const res = await fetch('/api/recording-status');
        if (!res.ok) {
            throw new Error(`HTTP ${res.status}`);
        }
        const status = await res.json();
        // Handle both response formats
        const recording = status.is_recording === true || status.is_recording === 'true';
        const processCount = status.process_count || 0;
        updateRecordingButton(recording, processCount, status.message);
        isRecording = recording;
        if (isRecording) {
            startDataRefresh();
        } else {
            stopDataRefresh();
        }
    } catch (error) {
        console.error('Error checking recording status:', error);
        updateRecordingButton(false, 0, 'Error checking status');
        isRecording = false;
        stopDataRefresh();
    }
}

function startDataRefresh() {
    if (dataRefreshInterval) return;
    dataRefreshInterval = setInterval(() => {
        // Only skip if user is actively editing a cell right now
        if (isEditingCell || (document.activeElement && document.activeElement.contentEditable === 'true')) {
            return;
        }
        loadData({ light: true, force: true });
    }, RECORDING_REFRESH_MS);
}

function stopDataRefresh() {
    if (!dataRefreshInterval) return;
    clearInterval(dataRefreshInterval);
    dataRefreshInterval = null;
}

function manualRefresh() {
    loadData({ light: true });
}

// Update recording button visual state
function updateRecordingButton(recording, processCount, message) {
    const recordBtn = document.getElementById('record-btn');
    const indicator = document.getElementById('recording-indicator');
    const statusText = document.getElementById('recording-status-text');
    
    if (!recordBtn || !indicator || !statusText) {
        return;
    }
    
    // Remove existing onclick attribute
    recordBtn.removeAttribute('onclick');
    
    if (recording) {
        // Show red dot indicator
        indicator.style.display = 'block';
        
        // Update button
        let btnText = 'Stop Recording';
        if (processCount > 1) {
            btnText += ` (⚠️ ${processCount})`;
            recordBtn.style.background = '#ff9800'; // Orange warning
            indicator.style.background = '#ff9800'; // Orange warning
        } else {
            recordBtn.style.background = '#28a745'; // Green
            indicator.style.background = '#dc3545'; // Red dot
        }
        recordBtn.innerHTML = btnText;
        recordBtn.style.color = '#ffffff';
        recordBtn.style.cursor = 'pointer';
        recordBtn.style.border = 'none';
        recordBtn.style.fontWeight = 'bold';
        recordBtn.onclick = function(e) { e.preventDefault(); stopRecording(); };
        recordBtn.disabled = false;
        
        // Update status text
        if (processCount > 1) {
            statusText.textContent = `⚠️ ${processCount} monitors running - may cause conflicts`;
            statusText.style.color = '#ff9800';
    } else {
            statusText.textContent = '🔴 Recording in progress';
            statusText.style.color = '#28a745';
        }
    } else {
        // Hide red dot indicator
        indicator.style.display = 'none';
        
        // Update button
        recordBtn.innerHTML = 'Record';
        recordBtn.style.background = '#dc3545';
        recordBtn.style.color = '#ffffff';
        recordBtn.style.cursor = 'pointer';
        recordBtn.style.border = 'none';
        recordBtn.style.fontWeight = 'bold';
        recordBtn.onclick = function(e) { e.preventDefault(); startRecording(); };
        recordBtn.disabled = false;
        
        // Update status text
        statusText.textContent = 'Not recording';
        statusText.style.color = 'var(--text-secondary)';
    }
}

// Start recording
async function startRecording() {
    if (!currentShowId) {
        alert('Please create or select a show first');
        return;
    }
    
    // Check if already recording
    if (isRecording) {
        alert('Recording is already in progress');
        return;
    }
    
    const recordBtn = document.getElementById('record-btn');
    if (recordBtn) {
        recordBtn.disabled = true;
        recordBtn.innerHTML = 'Starting...';
    }
    
    try {
        // Start monitor via API (monitor.py will open browser automatically)
        const res = await fetch('/api/start-recording', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({show_id: currentShowId})
        });
        
        const result = await res.json();
        
        if (res.ok) {
            // Start polling for status
            if (recordingStatusInterval) {
                clearInterval(recordingStatusInterval);
            }
            recordingStatusInterval = setInterval(checkRecordingStatus, 2000); // Check every 2 seconds
            
            // Check immediately
            setTimeout(checkRecordingStatus, 1000);
            
            // Show success message
            const showSelect = document.getElementById('show-select');
            const showName = showSelect ? showSelect.options[showSelect.selectedIndex]?.text : 'the selected show';
            alert(`Recording started for ${showName}!\n\nConnected to your TikTok Shop tab via CDP. Make sure your live event is open.`);
        } else {
            alert('Error starting recording: ' + (result.error || 'Unknown error'));
            if (recordBtn) {
                recordBtn.disabled = false;
                recordBtn.innerHTML = 'Record';
            }
        }
    } catch (error) {
        alert('Error starting recording: ' + error.message);
        if (recordBtn) {
            recordBtn.disabled = false;
            recordBtn.innerHTML = 'Record';
        }
    }
}

// Stop recording
async function stopRecording() {
    if (!confirm('Are you sure you want to stop recording?')) {
        return;
    }
    
    const recordBtn = document.getElementById('record-btn');
    if (recordBtn) {
        recordBtn.disabled = true;
        recordBtn.innerHTML = 'Stopping...';
    }
    
    try {
        const res = await fetch('/api/stop-recording', {
            method: 'POST'
        });
        
        const result = await res.json();
        
        if (res.ok && result.success) {
            // Stop polling
            if (recordingStatusInterval) {
                clearInterval(recordingStatusInterval);
                recordingStatusInterval = null;
            }
            updateRecordingButton(false);
            isRecording = false;
            stopDataRefresh();
            showToast('Recording stopped', 'success');
        } else {
            showToast('Error stopping recording: ' + (result.error || result.message || 'Unknown error'), 'error', 5000);
        }
    } catch (error) {
        showToast('Error stopping recording: ' + error.message, 'error', 5000);
    } finally {
        if (recordBtn) {
            recordBtn.disabled = false;
        }
    }
}

// Load data (preserves user input across refreshes)
async function loadData(options = {}) {
    const inputStates = (typeof saveInputStates === 'function') ? saveInputStates() : null;
    const force = options.force === true;
    const light = options.light === true;
    if (!force) {
        // Only skip refresh if actively editing a cell or have unsaved edits
        if (
            isEditingCell ||
            (document.activeElement && document.activeElement.contentEditable === 'true') ||
            (typeof isSelecting !== 'undefined' && isSelecting) ||
            (typeof pendingEdits !== 'undefined' && pendingEdits.size > 0)
        ) {
            return;
        }
    }
    const showChanged = lastLoadedShowId !== currentShowId;
    const prevScroll = window.scrollY || 0;
    if (!force) {
        lastSelectionSnapshot = captureSelectionState();
    }
    if (showChanged) {
        aiResultsByItem = new Map();
        aiLastRunStats = null;
        aiLastRunTotal = null;
        aiSortOverrides = new Map();
        lastItemsFingerprint = null;
        updateAiControls();
    }
    try {
        // Include show_id in items request if available
        const itemsUrl = currentShowId ? `/api/items?show_id=${currentShowId}` : '/api/items';
        if (light) {
            const itemsRes = await fetch(itemsUrl);
            items = await itemsRes.json();
        } else {
            const [itemsRes, presetsRes, skusRes] = await Promise.all([
                fetch(itemsUrl),
                fetch('/api/presets'),
                fetch('/api/skus')
            ]);
            items = await itemsRes.json();
            presets = await presetsRes.json();
            const skusData = await skusRes.json();
            skuOptions = (skusData.skus || []).map(row => row.sku).filter(Boolean);
            skuPresetMap = new Map(
                (skusData.skus || [])
                    .filter(row => row.sku && row.preset_name)
                    .map(row => [row.sku.toLowerCase(), row.preset_name])
            );
        }
        if (light) {
            const lastItem = items && items.length ? items[items.length - 1] : null;
            const fingerprint = JSON.stringify({
                count: items.length || 0,
                last: lastItem
                    ? [
                          lastItem.item_name || '',
                          lastItem.timestamp || '',
                          lastItem.sold_price || '',
                          lastItem.cost || ''
                      ].join('|')
                    : ''
            });
            if (fingerprint === lastItemsFingerprint) {
                return;
            }
            lastItemsFingerprint = fingerprint;
        }
        applySavedFilterState();
        if (light && !showChanged) {
            diffUpdateTable();
        } else {
            renderTable();
        }
        renderPresets();
        updateTotals(); // This calls updateTfootTotals() internally
        lastLoadedShowId = currentShowId;
        if (!light) {
            const lastItem = items && items.length ? items[items.length - 1] : null;
            lastItemsFingerprint = JSON.stringify({
                count: items.length || 0,
                last: lastItem
                    ? [
                          lastItem.item_name || '',
                          lastItem.timestamp || '',
                          lastItem.sold_price || '',
                          lastItem.cost || ''
                      ].join('|')
                    : ''
            });
        }
        if (!force && lastSelectionSnapshot) {
            restoreSelectionState(lastSelectionSnapshot);
        }
        requestAnimationFrame(() => {
            window.scrollTo(0, prevScroll);
        });
        // Restore user input states that were saved before the reload
        if (inputStates && typeof restoreInputStates === 'function') {
            setTimeout(() => {
                restoreInputStates(inputStates);
                if (typeof updateSelectedCount === 'function') updateSelectedCount();
            }, 100);
        }
    } catch (error) {
        console.error('Error loading data:', error);
        const tbody = document.getElementById('items-table-body');
        if (tbody) {
            tbody.innerHTML = `<tr><td colspan="${columnOrder.length}">Error loading data. Make sure log.csv exists in captures/ folder.</td></tr>`;
        }
        // Ensure tfoot exists even on error
        updateTfootTotals();
        requestAnimationFrame(() => {
            window.scrollTo(0, prevScroll);
        });
    }
}

function saveScrollPosition() {
    try {
        localStorage.setItem(SCROLL_POS_KEY, String(window.scrollY || 0));
    } catch (e) {
        // Ignore storage errors
    }
}

function captureSelectionState() {
    const rowNames = [];
    selectedRowIndices.forEach((index) => {
        const item = displayItems[index];
        if (item && item.item_name) {
            rowNames.push(item.item_name);
        }
    });
    const cellKeys = [];
    if (selectedCells && selectedCells.length > 0) {
        selectedCells.forEach((cell) => {
            const key = cell?.getAttribute('data-item-key');
            const field = cell?.getAttribute('data-field');
            if (key && field) {
                cellKeys.push({ key, field });
            }
        });
    }
    return { rowNames, cellKeys };
}

function restoreSelectionState(snapshot) {
    if (!snapshot) return;
    selectedRowIndices.clear();
    if (snapshot.rowNames && snapshot.rowNames.length > 0) {
        displayItems.forEach((item, index) => {
            if (snapshot.rowNames.includes(item.item_name)) {
                selectedRowIndices.add(index);
            }
        });
    }
    updateRowSelection();
    selectedCells = [];
    if (snapshot.cellKeys && snapshot.cellKeys.length > 0) {
        snapshot.cellKeys.forEach(({ key, field }) => {
            const cell = document.querySelector(
                `.editable-cell[data-item-key="${key}"][data-field="${field}"]`
            );
            if (cell) {
                cell.classList.add('selected');
                selectedCells.push(cell);
            }
        });
        updateSelectionStats();
    }
}

function restoreScrollPosition() {
    try {
        const saved = localStorage.getItem(SCROLL_POS_KEY);
        if (saved !== null) {
            const y = parseInt(saved, 10);
            if (!isNaN(y)) {
                window.scrollTo(0, y);
            }
        }
    } catch (e) {
        // Ignore storage errors
    }
}

window.addEventListener('beforeunload', saveScrollPosition);
window.addEventListener('scroll', () => {
    if (scrollSaveTimer) {
        clearTimeout(scrollSaveTimer);
    }
    scrollSaveTimer = setTimeout(saveScrollPosition, 150);
});

function getSortValueForField(item, field) {
    if (!item) return '';
    const overrides = aiSortOverrides.get(item.item_name);
    if (overrides && overrides[field] && !(item[field] || '')) {
        return overrides[field];
    }
    if (field === 'sku') {
        const current = item.sku || '';
        if (current) return current;
        if (!aiIncludeSku || isAiDismissed(item.item_name, 'sku')) return '';
        const suggestion = getAiSuggestion(item.item_name);
        return suggestion && suggestion.sku ? suggestion.sku : '';
    }
    if (field === 'preset_name') {
        const current = item.preset_name || '';
        if (current) return current;
        if (isAiDismissed(item.item_name, 'preset')) return '';
        const suggestion = getAiSuggestion(item.item_name);
        return suggestion && suggestion.preset_name ? suggestion.preset_name : '';
    }
    return item[field] || '';
}

function setAiStatus(text) {
    const statusEl = document.getElementById('ai-status');
    if (statusEl) statusEl.textContent = text;
}

function setAiSummary(text) {
    const summaryEl = document.getElementById('ai-summary');
    if (summaryEl) summaryEl.textContent = text || '';
}

function updateAiControls() {
    const toggle = document.getElementById('ai-toggle');
    const scanBtn = document.getElementById('ai-scan-btn');
    const applyBtn = document.getElementById('ai-apply-btn');
    const onlyBlanksEl = document.getElementById('ai-only-blanks');
    const includeSkuEl = document.getElementById('ai-include-sku');
    const matchOnlyEl = document.getElementById('ai-match-only');
    if (toggle) toggle.checked = aiEnabled;
    if (onlyBlanksEl) onlyBlanksEl.checked = aiOnlyBlanks;
    if (includeSkuEl) includeSkuEl.checked = aiIncludeSku;
    if (matchOnlyEl) matchOnlyEl.checked = aiMatchOnly;
    const canRun = aiEnabled && currentShowId && !aiRunning;
    if (scanBtn) scanBtn.disabled = !canRun;
    if (applyBtn) {
        applyBtn.disabled = !aiEnabled || !currentShowId || aiRunning;
    }
    if (!aiEnabled) {
        setAiStatus('AI off');
    } else if (!currentShowId) {
        setAiStatus('Pick a show to use AI');
    } else if (aiRunning) {
        setAiStatus('AI is working...');
    } else if (aiResultsByItem.size > 0) {
        setAiStatus(`${aiResultsByItem.size} AI match${aiResultsByItem.size === 1 ? '' : 'es'} ready`);
    } else if (aiLastRunStats) {
        setAiStatus('AI scan complete');
    } else {
        setAiStatus('AI ready (run Scan first)');
    }

    if (aiLastRunTotal !== null) {
        const matches = aiResultsByItem.size;
        const percent = aiLastRunTotal > 0
            ? Math.round((matches / aiLastRunTotal) * 100)
            : 0;
        setAiSummary(`${matches}/${aiLastRunTotal} matched (${percent}%)`);
    } else {
        setAiSummary('');
    }
}

function initAiControls() {
    const toggle = document.getElementById('ai-toggle');
    const onlyBlanksEl = document.getElementById('ai-only-blanks');
    const includeSkuEl = document.getElementById('ai-include-sku');
    const matchOnlyEl = document.getElementById('ai-match-only');
    if (toggle) {
        toggle.checked = aiEnabled;
        toggle.addEventListener('change', () => {
            aiEnabled = toggle.checked;
            localStorage.setItem(AI_ENABLED_KEY, aiEnabled.toString());
            if (!aiEnabled) {
                aiResultsByItem = new Map();
                aiLastRunStats = null;
                renderTable();
            }
            updateAiControls();
        });
    }
    if (onlyBlanksEl) {
        onlyBlanksEl.checked = aiOnlyBlanks;
        onlyBlanksEl.addEventListener('change', () => {
            aiOnlyBlanks = onlyBlanksEl.checked;
            localStorage.setItem(AI_ONLY_BLANKS_KEY, aiOnlyBlanks.toString());
        });
    }
    if (includeSkuEl) {
        includeSkuEl.checked = aiIncludeSku;
        includeSkuEl.addEventListener('change', () => {
            aiIncludeSku = includeSkuEl.checked;
            localStorage.setItem(AI_INCLUDE_SKU_KEY, aiIncludeSku.toString());
            renderTable();
        });
    }
    if (matchOnlyEl) {
        matchOnlyEl.checked = aiMatchOnly;
        matchOnlyEl.addEventListener('change', () => {
            aiMatchOnly = matchOnlyEl.checked;
            localStorage.setItem(AI_MATCH_ONLY_KEY, aiMatchOnly.toString());
            renderTable();
        });
    }
    updateAiControls();
}

function formatAiConfidence(value) {
    if (value === null || value === undefined) return '';
    const percent = Math.round(value * 100);
    return `${percent}%`;
}

function getAiSuggestion(itemName) {
    return aiResultsByItem.get(itemName);
}

function isAiDismissed(itemName, field) {
    if (!itemName || !field) return false;
    const fields = aiDismissed.get(itemName);
    return fields ? fields.has(field) : false;
}

function dismissAiSuggestion(itemName, field) {
    if (!itemName || !field) return;
    const result = aiResultsByItem.get(itemName);
    const item = items.find(i => i.item_name === itemName);
    const sortField = field === 'preset' ? 'preset_name' : field;
    if (item && sortColumn === sortField) {
        const currentSortValue = getSortValueForField(item, sortField);
        if (currentSortValue && (item[sortField] || '') === '') {
            if (!aiSortOverrides.has(itemName)) {
                aiSortOverrides.set(itemName, {});
            }
            aiSortOverrides.get(itemName)[sortField] = currentSortValue;
        }
    }
    if (result && result.source_image && result.matched_image) {
        fetch('/api/ai/feedback', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                action: 'reject',
                show_id: currentShowId,
                source_image: result.source_image,
                matched_image: result.matched_image
            })
        }).catch(err => {
            console.error('AI feedback error:', err);
        });
    }
    if (!aiDismissed.has(itemName)) {
        aiDismissed.set(itemName, new Set());
    }
    aiDismissed.get(itemName).add(field);
    const prevScroll = window.scrollY || 0;
    renderTable();
    updateAiControls();
    requestAnimationFrame(() => {
        window.scrollTo(0, prevScroll);
    });
}

function getAiTargetItems() {
    const selected = getSelectedItems();
    const useSelection = selected && selected.length > 0;
    const source = useSelection
        ? selected.map(index => displayItems[index]).filter(Boolean)
        : displayItems.slice();
    return { items: source, scopeLabel: useSelection ? 'selection' : 'visible rows' };
}

function isGiveawayPreset(presetName) {
    if (!presetName) return false;
    const match = presets.find(preset => preset.name === presetName);
    return match ? !!match.is_giveaway : false;
}

function learnAiFromItem(itemName) {
    if (!itemName || !currentShowId) return;
    const item = items.find(i => i.item_name === itemName);
    if (!item || !item.image) return;
    const sku = item.sku || null;
    const presetName = item.preset_name || null;
    if (!sku && !presetName) return;
    fetch('/api/ai/learn', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            show_id: currentShowId,
            item_name: itemName,
            image: item.image,
            sku,
            preset_name: presetName
        })
    }).catch(err => {
        console.error('AI learn error:', err);
    });
}

async function runAiScan() {
    if (!aiEnabled) {
        alert('Turn on AI Mode first.');
        return;
    }
    if (!currentShowId) {
        alert('Please select a show first.');
        return;
    }
    const { items: targetItems, scopeLabel } = getAiTargetItems();
    if (!targetItems.length) {
        alert('No rows to scan.');
        return;
    }
    aiRunning = true;
    updateAiControls();
    try {
        setAiStatus('Building AI index (first run can take a few minutes)...');
        const buildRes = await fetch('/api/ai/build-index', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ days: 0 })
        });
        if (!buildRes.ok) {
            const error = await buildRes.json();
            throw new Error(error.error || 'Failed to build index');
        }

        setAiStatus('Scanning images...');
        const payload = targetItems.map(item => ({
            item_name: item.item_name,
            image: item.image
        }));
        const res = await fetch('/api/ai/run', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                show_id: currentShowId,
                items: payload,
                only_fill_blanks: aiOnlyBlanks
            })
        });
        const data = await res.json();
        if (!res.ok) {
            throw new Error(data.error || 'AI scan failed');
        }
        aiResultsByItem = new Map(
            (data.results || []).map(result => [result.item_name, result])
        );
        aiLastRunStats = data.stats || null;
        aiLastRunTotal = targetItems.length;
        aiDismissed = new Map();
        aiSortOverrides = new Map();
        renderTable();
        if (aiResultsByItem.size === 0) {
            setAiStatus(`No AI matches found for ${scopeLabel}`);
        } else {
            setAiStatus(`${aiResultsByItem.size} match${aiResultsByItem.size === 1 ? '' : 'es'} found for ${scopeLabel}`);
        }
    } catch (error) {
        console.error('AI scan error:', error);
        alert('AI scan error: ' + error.message);
        setAiStatus('AI scan failed');
    } finally {
        aiRunning = false;
        updateAiControls();
    }
}

async function applyAiResults() {
    if (!aiEnabled) {
        alert('Turn on AI Mode first.');
        return;
    }
    if (!currentShowId) {
        alert('Please select a show first.');
        return;
    }
    const { items: targetItems } = getAiTargetItems();
    const results = targetItems
        .map(item => {
            const result = aiResultsByItem.get(item.item_name);
            if (!result) return null;
            const dismissedSku = isAiDismissed(item.item_name, 'sku');
            const dismissedPreset = isAiDismissed(item.item_name, 'preset');
            if (dismissedSku && dismissedPreset) return null;
            return {
                ...result,
                sku: dismissedSku ? null : result.sku,
                preset_name: dismissedPreset ? null : result.preset_name
            };
        })
        .filter(Boolean);
    if (!results.length) {
        setAiStatus('No AI results to apply. Run Scan first.');
        alert('No AI results to apply for the current view. Run Scan first.');
        return;
    }
    aiRunning = true;
    updateAiControls();
    try {
        setAiStatus('Applying AI results...');
        const res = await fetch('/api/ai/apply', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                show_id: currentShowId,
                results,
                only_fill_blanks: aiOnlyBlanks,
                include_sku: aiIncludeSku
            })
        });
        const data = await res.json();
        if (!res.ok) {
            throw new Error(data.error || 'Failed to apply AI results');
        }
        setAiStatus(`Applied ${data.updated} item${data.updated === 1 ? '' : 's'}`);
        aiResultsByItem = new Map();
        aiLastRunStats = null;
        loadData();
    } catch (error) {
        console.error('AI apply error:', error);
        alert('AI apply error: ' + error.message);
        setAiStatus('AI apply failed');
    } finally {
        aiRunning = false;
        updateAiControls();
    }
}

// Apply sorting to items array
function applySorting() {
    if (!sortColumn) return;
    
    items.sort((a, b) => {
        let aVal, bVal;
        
        // Get values based on sort column
        if (sortColumn === 'item_name') {
            aVal = a.item_name || '';
            bVal = b.item_name || '';
        } else if (sortColumn === 'sold_timestamp') {
            aVal = a.sold_timestamp || '';
            bVal = b.sold_timestamp || '';
        } else if (sortColumn === 'viewers') {
            aVal = a.viewers ? parseInt(a.viewers, 10) : -Infinity;
            bVal = b.viewers ? parseInt(b.viewers, 10) : -Infinity;
            if (isNaN(aVal)) aVal = -Infinity;
            if (isNaN(bVal)) bVal = -Infinity;
        } else if (sortColumn === 'order_id') {
            aVal = a.order_id ? parseInt(a.order_id, 10) : -Infinity;
            bVal = b.order_id ? parseInt(b.order_id, 10) : -Infinity;
            if (isNaN(aVal)) aVal = -Infinity;
            if (isNaN(bVal)) bVal = -Infinity;
        } else if (sortColumn === 'buyer') {
            aVal = a.buyer || '';
            bVal = b.buyer || '';
        } else if (sortColumn === 'cancelled_status') {
            aVal = a.cancelled_status || '';
            bVal = b.cancelled_status || '';
        } else if (sortColumn === 'sku') {
            aVal = getSortValueForField(a, 'sku');
            bVal = getSortValueForField(b, 'sku');
        } else if (sortColumn === 'notes') {
            aVal = a.notes || '';
            bVal = b.notes || '';
        } else if (sortColumn === 'preset_name') {
            aVal = getSortValueForField(a, 'preset_name');
            bVal = getSortValueForField(b, 'preset_name');
        } else if (sortColumn === 'pinned_message') {
            aVal = a.pinned_message || '';
            bVal = b.pinned_message || '';
        } else if (sortColumn === 'sold_price') {
            aVal = a.sold_price_float !== null && a.sold_price_float !== undefined ? a.sold_price_float : -Infinity;
            bVal = b.sold_price_float !== null && b.sold_price_float !== undefined ? b.sold_price_float : -Infinity;
        } else if (sortColumn === 'net_revenue') {
            const aNet = calcNetRevenue(a.sold_price_float, a.preset_name);
            const bNet = calcNetRevenue(b.sold_price_float, b.preset_name);
            aVal = aNet !== null ? aNet : -Infinity;
            bVal = bNet !== null ? bNet : -Infinity;
        } else if (sortColumn === 'cost') {
            aVal = a.cost !== null && a.cost !== undefined ? a.cost : -Infinity;
            bVal = b.cost !== null && b.cost !== undefined ? b.cost : -Infinity;
        } else if (sortColumn === 'profit') {
            const aProf = calcProfit(a.sold_price_float, a.cost, a.preset_name);
            const bProf = calcProfit(b.sold_price_float, b.cost, b.preset_name);
            aVal = aProf !== null ? aProf : -Infinity;
            bVal = bProf !== null ? bProf : -Infinity;
        } else {
            return 0;
        }
        
        // Compare values
        if (sortColumn === 'preset_name') {
            const nameResult = aVal.localeCompare(bVal);
            if (nameResult !== 0) {
                return sortDirection === 'asc' ? nameResult : -nameResult;
            }
            const aSku = (a.sku || '').trim();
            const bSku = (b.sku || '').trim();
            const aHasSku = aSku !== '';
            const bHasSku = bSku !== '';
            if (aHasSku !== bHasSku) {
                return aHasSku ? -1 : 1;
            }
            const skuResult = aSku.localeCompare(bSku);
            return sortDirection === 'asc' ? skuResult : -skuResult;
        }
        if (typeof aVal === 'string' && typeof bVal === 'string') {
            const result = sortColumn === 'item_name'
                ? naturalSort.compare(aVal, bVal)
                : aVal.localeCompare(bVal);
            return sortDirection === 'asc' ? result : -result;
        } else {
            const result = aVal - bVal;
            return sortDirection === 'asc' ? result : -result;
        }
    });
}

// Sort table by column
function sortTable(column) {
    // If editing, save first, then sort
    const active = document.activeElement;
    if (active && active.contentEditable === 'true') {
        active.blur();
        setTimeout(() => sortTable(column), 0);
        return;
    }
    sortClickLockUntil = Date.now() + 300;
    clearColumnInteractionsIfIdle();
    applyPendingEditsToItems();
    if (sortColumn === column) {
        // Toggle direction
        sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
    } else {
        // New column
        sortColumn = column;
        sortDirection = 'asc';
    }
    saveSortState();
    setDebugEvent(`sortTable(${column})`);
    
    // Update visual indicators (will be done in renderTable)
    applySorting();
    renderTable();
}

function getPresetUsageCounts() {
    const counts = new Map();
    let filledCount = 0;
    const total = items.length;
    items.forEach(item => {
        const name = (item.preset_name || '').trim();
        if (!name) {
            counts.set(PRESET_BLANK, (counts.get(PRESET_BLANK) || 0) + 1);
        } else {
            counts.set(name, (counts.get(name) || 0) + 1);
            filledCount += 1;
        }
    });
    return { counts, total, filledCount };
}

function getPresetGroupOptions() {
    const { counts, total } = getPresetUsageCounts();
    const groupMap = new Map();
    presets.forEach(preset => {
        const groups = preset.groups || [];
        groups.forEach(group => {
            if (!group || !group.id) return;
            if (!groupMap.has(group.id)) {
                groupMap.set(group.id, { id: group.id, name: group.name, presetNames: new Set() });
            }
            groupMap.get(group.id).presetNames.add(preset.name);
        });
    });
    const giveawayPresets = presets
        .filter(preset => preset.is_giveaway)
        .map(preset => preset.name);
    if (giveawayPresets.length) {
        groupMap.set('giveaway', {
            id: 'giveaway',
            name: 'Giveaways',
            presetNames: new Set(giveawayPresets)
        });
    }

    return Array.from(groupMap.values())
        .sort((a, b) => a.name.localeCompare(b.name))
        .map(group => {
            let count = 0;
            group.presetNames.forEach(name => {
                count += counts.get(name) || 0;
            });
            return {
                value: `${PRESET_GROUP_PREFIX}${group.id}`,
                groupId: group.id,
                label: `Group: ${group.name}`,
                count,
                total,
                presetNames: Array.from(group.presetNames)
            };
        });
}

function getPresetOptionList() {
    const { counts, total } = getPresetUsageCounts();
    const options = [];
    const blankCount = counts.get(PRESET_BLANK) || 0;
    options.push({ value: PRESET_BLANK, label: '(Blank)', count: blankCount, total });

    const names = Array.from(counts.keys())
        .filter(name => name !== PRESET_BLANK)
        .sort((a, b) => a.localeCompare(b));

    names.forEach(name => {
        options.push({ value: name, label: name, count: counts.get(name) || 0, total });
    });

    return options;
}

function ensurePresetFilterTemp() {
    if (presetFilterTemp) return;
    const options = getPresetOptionList();
    const allValues = options.map(opt => opt.value);
    if (presetFilterSelected) {
        presetFilterTemp = new Set(Array.from(presetFilterSelected));
    } else {
        presetFilterTemp = new Set(allValues);
    }
}

function ensurePresetFilterGroupTemp() {
    if (presetFilterGroupTemp) return;
    presetFilterGroupTemp = new Set(Array.from(presetFilterGroupSelected));
}

function getCancelledStatusCounts() {
    let cancelledCount = 0;
    let failedCount = 0;
    let processedCount = 0;
    const total = items.length;

    items.forEach(item => {
        const status = (item.cancelled_status || '').trim();
        if (status === STATUS_CANCELLED) {
            cancelledCount += 1;
        } else if (status === STATUS_FAILED) {
            failedCount += 1;
        } else {
            processedCount += 1;
        }
    });

    return { cancelledCount, failedCount, processedCount, total };
}

function getCancelledStatusOptions() {
    const { cancelledCount, failedCount, processedCount, total } = getCancelledStatusCounts();
    return [
        { value: STATUS_PROCESSED, label: 'Processed', count: processedCount, total },
        { value: STATUS_CANCELLED, label: 'Cancelled', count: cancelledCount, total },
        { value: STATUS_FAILED, label: 'Failed', count: failedCount, total }
    ];
}

function ensureCancelledFilterTemp() {
    if (cancelledFilterTemp) return;
    const options = getCancelledStatusOptions();
    const allValues = options.map(opt => opt.value);
    if (cancelledFilterSelected) {
        cancelledFilterTemp = new Set(Array.from(cancelledFilterSelected));
    } else {
        cancelledFilterTemp = new Set([STATUS_PROCESSED]);
    }
}

function renderPresetFilterOptions() {
    const optionsEl = document.getElementById('preset-filter-options');
    const searchEl = document.getElementById('preset-filter-search');
    if (!optionsEl) return;

    const options = getPresetOptionList();
    const groupOptions = getPresetGroupOptions();
    const allValues = options.map(opt => opt.value);

    ensurePresetFilterTemp();
    ensurePresetFilterGroupTemp();

    const query = (searchEl ? searchEl.value : '').trim().toLowerCase();
    const filtered = options.filter(opt => {
        if (!query) return true;
        return opt.label.toLowerCase().includes(query);
    });
    const filteredGroups = groupOptions.filter(opt => {
        if (!query) return true;
        return opt.label.toLowerCase().includes(query);
    });

    const allSelected = allValues.length > 0 && allValues.every(val => presetFilterTemp.has(val));
    const selectAllRow = `
        <div class="preset-filter-option">
            <label>
                <input type="checkbox" data-value="__all__" ${allSelected ? 'checked' : ''}>
                <span>Select all</span>
            </label>
            <span class="preset-filter-option-count">${allValues.length}/${allValues.length}</span>
        </div>
    `;

    if (options.length === 0) {
        optionsEl.innerHTML = `<div class="preset-filter-option">No presets yet.</div>`;
        return;
    }

    const groupRows = filteredGroups.map(opt => {
        const isChecked = presetFilterGroupTemp.has(String(opt.groupId));
        return `
            <div class="preset-filter-option preset-filter-group-option">
                <label>
                    <input type="checkbox" data-group-id="${opt.groupId}" ${isChecked ? 'checked' : ''}>
                    <span>${escapeHtml(opt.label)}</span>
                </label>
                <span class="preset-filter-option-count">${opt.count}/${opt.total}</span>
            </div>
        `;
    }).join('');

    const presetRows = filtered.map(opt => `
        <div class="preset-filter-option">
            <label>
                <input type="checkbox" data-value="${escapeHtml(opt.value)}" ${presetFilterTemp.has(opt.value) ? 'checked' : ''}>
                <span>${escapeHtml(opt.label)}</span>
            </label>
            <span class="preset-filter-option-count">${opt.count}/${opt.total}</span>
        </div>
    `).join('');

    optionsEl.innerHTML = selectAllRow + groupRows + presetRows;

}

function renderCancelledFilterOptions() {
    const optionsEl = document.getElementById('cancelled-filter-options');
    const searchEl = document.getElementById('cancelled-filter-search');
    if (!optionsEl) return;

    const options = getCancelledStatusOptions();
    const allValues = options.map(opt => opt.value);

    ensureCancelledFilterTemp();

    const query = (searchEl ? searchEl.value : '').trim().toLowerCase();
    const filtered = options.filter(opt => {
        if (!query) return true;
        return opt.label.toLowerCase().includes(query);
    });

    const allSelected = allValues.length > 0 && allValues.every(val => cancelledFilterTemp.has(val));
    const selectAllRow = `
        <div class="preset-filter-option">
            <label>
                <input type="checkbox" data-value="__all__" ${allSelected ? 'checked' : ''}>
                <span>Select all</span>
            </label>
            <span class="preset-filter-option-count">${allValues.length}/${allValues.length}</span>
        </div>
    `;

    if (options.length === 0) {
        optionsEl.innerHTML = `<div class="preset-filter-option">No statuses yet.</div>`;
        return;
    }

    optionsEl.innerHTML = selectAllRow + filtered.map(opt => `
        <div class="preset-filter-option">
            <label>
                <input type="checkbox" data-value="${escapeHtml(opt.value)}" ${cancelledFilterTemp.has(opt.value) ? 'checked' : ''}>
                <span>${escapeHtml(opt.label)}</span>
            </label>
            <span class="preset-filter-option-count">${opt.count}/${opt.total}</span>
        </div>
    `).join('');
}

function updatePresetFilterCount() {
    const countEl = document.getElementById('preset-filter-count');
    if (!countEl) return;
    const { counts, total, filledCount } = getPresetUsageCounts();
    const totalText = `${filledCount}/${total}`;

    if (!presetFilterSelected) {
        countEl.textContent = totalText;
        return;
    }

    const selected = Array.from(presetFilterSelected);
    if (selected.length === 1) {
        const value = selected[0];
        const label = value === PRESET_BLANK ? '(Blank)' : value;
        const count = value === PRESET_BLANK ? (counts.get(PRESET_BLANK) || 0) : (counts.get(value) || 0);
        countEl.textContent = `${label} ${count}/${total}`;
        return;
    }

    countEl.textContent = `${selected.length} selected • ${totalText}`;
}

function updateCancelledFilterCount() {
    const countEl = document.getElementById('cancelled-filter-count');
    if (!countEl) return;
    const { cancelledCount, failedCount, processedCount, total } = getCancelledStatusCounts();
    const totalText = `${processedCount + cancelledCount + failedCount}/${total}`;

    if (!cancelledFilterSelected) {
        countEl.textContent = totalText;
        return;
    }

    const selected = Array.from(cancelledFilterSelected);
    if (selected.length === 1) {
        const value = selected[0];
        let count = 0;
        if (value === STATUS_PROCESSED) count = processedCount;
        if (value === STATUS_CANCELLED) count = cancelledCount;
        if (value === STATUS_FAILED) count = failedCount;
        countEl.textContent = `${value} ${count}/${total}`;
        return;
    }

    countEl.textContent = `${selected.length} selected • ${totalText}`;
}

function getSkuUsageCounts() {
    const counts = new Map();
    const total = items.length;
    items.forEach(item => {
        const sku = (item.sku || '').trim();
        if (!sku) {
            counts.set(SKU_BLANK, (counts.get(SKU_BLANK) || 0) + 1);
        } else {
            counts.set(sku, (counts.get(sku) || 0) + 1);
        }
    });
    return { counts, total };
}

function getSkuOptionList() {
    const { counts, total } = getSkuUsageCounts();
    const options = [];
    const blankCount = counts.get(SKU_BLANK) || 0;
    options.push({ value: SKU_BLANK, label: '(Blank)', count: blankCount, total });
    const names = Array.from(counts.keys())
        .filter(name => name !== SKU_BLANK)
        .sort((a, b) => a.localeCompare(b));
    names.forEach(name => {
        options.push({ value: name, label: name, count: counts.get(name) || 0, total });
    });
    return options;
}

function ensureSkuFilterTemp() {
    if (skuFilterTemp) return;
    const options = getSkuOptionList();
    const allValues = options.map(opt => opt.value);
    if (skuFilterSelected) {
        skuFilterTemp = new Set(Array.from(skuFilterSelected));
    } else {
        skuFilterTemp = new Set(allValues);
    }
}

function updateSkuFilterCount() {
    const countEl = document.getElementById('sku-filter-count');
    if (!countEl) return;
    const { counts, total } = getSkuUsageCounts();
    const totalText = `${total}/${total}`;
    if (!skuFilterSelected) {
        countEl.textContent = totalText;
        return;
    }
    const selected = Array.from(skuFilterSelected);
    if (selected.length === 1) {
        const value = selected[0];
        const label = value === SKU_BLANK ? '(Blank)' : value;
        const count = value === SKU_BLANK ? (counts.get(SKU_BLANK) || 0) : (counts.get(value) || 0);
        countEl.textContent = `${label} ${count}/${total}`;
        return;
    }
    countEl.textContent = `${selected.length} selected • ${totalText}`;
}

function getSkuOptionsMarkup(currentValue) {
    const options = new Set(
        skuOptions
            .map(sku => (sku || '').trim())
            .filter(Boolean)
    );
    if (currentValue) {
        options.add(currentValue);
    }
    const sorted = Array.from(options).sort((a, b) => a.localeCompare(b));
    return ['<option value="">Select SKU...</option>']
        .concat(sorted.map(sku => {
            const selected = currentValue && sku === currentValue ? 'selected' : '';
            return `<option value="${escapeHtml(sku)}" ${selected}>${escapeHtml(sku)}</option>`;
        }))
        .join('');
}

function openPresetFilter() {
    const menu = document.getElementById('preset-filter-menu');
    if (!menu) return;
    presetFilterOpen = true;
    presetFilterTemp = null;
    presetFilterGroupTemp = null;
    renderPresetFilterOptions();
    menu.style.display = 'block';
}

function closePresetFilter(applyChanges) {
    const menu = document.getElementById('preset-filter-menu');
    if (menu) menu.style.display = 'none';
    presetFilterOpen = false;

    if (!applyChanges) {
        presetFilterTemp = null;
        presetFilterBlankToggled = false;
        presetFilterGroupTemp = null;
        return;
    }

    const options = getPresetOptionList();
    const allValues = options.map(opt => opt.value);
    const selected = presetFilterTemp ? Array.from(presetFilterTemp) : allValues;

    if (selected.length === allValues.length) {
        presetFilterSelected = null;
        presetFilterStickyItems = new Set();
    } else {
        presetFilterSelected = new Set(selected);
    }

    presetFilterTemp = null;
    if (presetFilterGroupTemp) {
        presetFilterGroupSelected = new Set(Array.from(presetFilterGroupTemp));
    }
    presetFilterGroupTemp = null;
    presetFilterStickyItems = new Set();
    presetFilterBlankToggled = false;
    saveFilterState();
    renderTable();
}

function openCancelledFilter() {
    const menu = document.getElementById('cancelled-filter-menu');
    if (!menu) return;
    cancelledFilterOpen = true;
    cancelledFilterTemp = null;
    renderCancelledFilterOptions();
    menu.style.display = 'block';
}

function openSkuFilter() {
    const menu = document.getElementById('sku-filter-menu');
    if (!menu) return;
    skuFilterOpen = true;
    skuFilterTemp = null;
    renderSkuFilterOptions();
    menu.style.display = 'block';
}

function closeCancelledFilter(applyChanges) {
    const menu = document.getElementById('cancelled-filter-menu');
    if (menu) menu.style.display = 'none';
    cancelledFilterOpen = false;

    if (!applyChanges) {
        cancelledFilterTemp = null;
        return;
    }
    cancelledFilterTemp = getCancelledFilterTempFromDOM();
    applyCancelledFilterTemp();
    cancelledFilterTemp = null;
}

function applyCancelledFilterTemp() {
    const options = getCancelledStatusOptions();
    const allValues = options.map(opt => opt.value);
    const selected = cancelledFilterTemp ? Array.from(cancelledFilterTemp) : allValues;

    if (selected.length === allValues.length) {
        cancelledFilterSelected = null;
    } else {
        cancelledFilterSelected = new Set(selected);
    }

    saveFilterState();
    renderTable();
}

function getCancelledFilterTempFromDOM() {
    const optionsEl = document.getElementById('cancelled-filter-options');
    if (!optionsEl) return cancelledFilterTemp;
    const selected = new Set();
    optionsEl.querySelectorAll('input[type="checkbox"]').forEach(input => {
        const value = input.dataset?.value;
        if (!value || value === '__all__') return;
        if (input.checked) selected.add(value);
    });
    return selected;
}

function closeSkuFilter(applyChanges) {
    const menu = document.getElementById('sku-filter-menu');
    if (menu) menu.style.display = 'none';
    skuFilterOpen = false;

    if (!applyChanges) {
        skuFilterTemp = null;
        return;
    }

    const options = getSkuOptionList();
    const allValues = options.map(opt => opt.value);
    const selected = skuFilterTemp ? Array.from(skuFilterTemp) : allValues;

    if (selected.length === allValues.length) {
        skuFilterSelected = null;
    } else {
        skuFilterSelected = new Set(selected);
    }

    skuFilterTemp = null;
    skuFilterStickyItems = new Set();
    saveFilterState();
    renderTable();
}

function togglePresetFilter(event) {
    event.preventDefault();
    event.stopPropagation();
    if (presetFilterOpen) {
        closePresetFilter(false);
    } else {
        openPresetFilter();
    }
}

function toggleCancelledFilter(event) {
    event.preventDefault();
    event.stopPropagation();
    if (cancelledFilterOpen) {
        closeCancelledFilter(false);
    } else {
        openCancelledFilter();
    }
}

function toggleSkuFilter(event) {
    event.preventDefault();
    event.stopPropagation();
    if (skuFilterOpen) {
        closeSkuFilter(false);
    } else {
        openSkuFilter();
    }
}

function initPresetFilter() {
    const menu = document.getElementById('preset-filter-menu');
    const toggle = document.getElementById('preset-filter-toggle');
    const okBtn = document.getElementById('preset-filter-ok');
    const cancelBtn = document.getElementById('preset-filter-cancel');
    const optionsEl = document.getElementById('preset-filter-options');
    const searchEl = document.getElementById('preset-filter-search');

    if (toggle && !toggle.hasAttribute('data-listener-attached')) {
        toggle.setAttribute('data-listener-attached', '1');
        toggle.addEventListener('click', togglePresetFilter);
    }
    if (okBtn && !okBtn.hasAttribute('data-listener-attached')) {
        okBtn.setAttribute('data-listener-attached', '1');
        okBtn.addEventListener('click', function(e) {
            e.preventDefault();
            closePresetFilter(true);
        });
    }
    if (cancelBtn && !cancelBtn.hasAttribute('data-listener-attached')) {
        cancelBtn.setAttribute('data-listener-attached', '1');
        cancelBtn.addEventListener('click', function(e) {
            e.preventDefault();
            closePresetFilter(false);
        });
    }
    if (optionsEl && !optionsEl.hasAttribute('data-listener-attached')) {
        optionsEl.setAttribute('data-listener-attached', '1');
        optionsEl.addEventListener('change', function(e) {
            const checkbox = e.target;
            if (!checkbox || !checkbox.dataset) return;
            const value = checkbox.dataset.value;
            const groupId = checkbox.dataset.groupId;
            ensurePresetFilterTemp();
            ensurePresetFilterGroupTemp();
            if (groupId) {
                const groups = getPresetGroupOptions();
                const group = groups.find(opt => String(opt.groupId) === String(groupId));
                if (!group) return;
                if (checkbox.checked) {
                    presetFilterGroupTemp.add(String(groupId));
                    group.presetNames.forEach(name => presetFilterTemp.add(name));
                } else {
                    presetFilterGroupTemp.delete(String(groupId));
                    group.presetNames.forEach(name => presetFilterTemp.delete(name));
                }
                renderPresetFilterOptions();
                return;
            }
            if (value === '__all__') {
                const options = getPresetOptionList();
                const allValues = options.map(opt => opt.value);
                if (checkbox.checked) {
                    presetFilterTemp = new Set(allValues);
                } else {
                    presetFilterTemp.clear();
                }
                presetFilterGroupTemp.clear();
                renderPresetFilterOptions();
                return;
            }
            if (value === PRESET_BLANK) {
                presetFilterBlankToggled = true;
            }
            if (checkbox.checked) {
                presetFilterTemp.add(value);
            } else {
                presetFilterTemp.delete(value);
            }
            renderPresetFilterOptions();
        });
    }
    if (searchEl && !searchEl.hasAttribute('data-listener-attached')) {
        searchEl.setAttribute('data-listener-attached', '1');
        searchEl.addEventListener('input', function() {
            renderPresetFilterOptions();
        });
    }

    updatePresetFilterCount();

    if (!presetFilterInitDone) {
        presetFilterInitDone = true;
        document.addEventListener('click', function(e) {
            if (!presetFilterOpen) return;
            const filterCell = document.getElementById('preset-filter-cell');
            if (filterCell && filterCell.contains(e.target)) {
                return;
            }
            closePresetFilter(false);
        });
    }
}

function initCancelledFilter() {
    const menu = document.getElementById('cancelled-filter-menu');
    const toggle = document.getElementById('cancelled-filter-toggle');
    const okBtn = document.getElementById('cancelled-filter-ok');
    const cancelBtn = document.getElementById('cancelled-filter-cancel');
    const optionsEl = document.getElementById('cancelled-filter-options');
    const searchEl = document.getElementById('cancelled-filter-search');

    if (toggle && !toggle.hasAttribute('data-listener-attached')) {
        toggle.setAttribute('data-listener-attached', '1');
        toggle.addEventListener('click', toggleCancelledFilter);
    }
    if (okBtn && !okBtn.hasAttribute('data-listener-attached')) {
        okBtn.setAttribute('data-listener-attached', '1');
        okBtn.addEventListener('click', function(e) {
            e.preventDefault();
            closeCancelledFilter(true);
        });
    }
    if (cancelBtn && !cancelBtn.hasAttribute('data-listener-attached')) {
        cancelBtn.setAttribute('data-listener-attached', '1');
        cancelBtn.addEventListener('click', function(e) {
            e.preventDefault();
            closeCancelledFilter(false);
        });
    }
    if (optionsEl && !optionsEl.hasAttribute('data-listener-attached')) {
        optionsEl.setAttribute('data-listener-attached', '1');
        optionsEl.addEventListener('change', function(e) {
            const checkbox = e.target;
            if (!checkbox || !checkbox.dataset || !checkbox.dataset.value) return;
            const value = checkbox.dataset.value;
            ensureCancelledFilterTemp();
            if (value === '__all__') {
                const options = getCancelledStatusOptions();
                const allValues = options.map(opt => opt.value);
                if (checkbox.checked) {
                    cancelledFilterTemp = new Set(allValues);
                } else {
                    cancelledFilterTemp.clear();
                }
                renderCancelledFilterOptions();
                cancelledFilterTemp = getCancelledFilterTempFromDOM();
                applyCancelledFilterTemp();
                return;
            }
            if (checkbox.checked) {
                cancelledFilterTemp.add(value);
            } else {
                cancelledFilterTemp.delete(value);
            }
            renderCancelledFilterOptions();
            cancelledFilterTemp = getCancelledFilterTempFromDOM();
            applyCancelledFilterTemp();
        });
    }
    if (searchEl && !searchEl.hasAttribute('data-listener-attached')) {
        searchEl.setAttribute('data-listener-attached', '1');
        searchEl.addEventListener('input', function() {
            renderCancelledFilterOptions();
        });
    }

    if (!cancelledFilterSelected && !cancelledFilterPersisted) {
        cancelledFilterSelected = new Set([STATUS_PROCESSED]);
        renderTable();
    }
    updateCancelledFilterCount();

    if (!cancelledFilterInitDone) {
        cancelledFilterInitDone = true;
        document.addEventListener('click', function(e) {
            if (!cancelledFilterOpen) return;
            const filterCell = document.getElementById('cancelled-filter-cell');
            if (filterCell && filterCell.contains(e.target)) {
                return;
            }
            closeCancelledFilter(false);
        });
    }
}

function renderSkuFilterOptions() {
    const optionsEl = document.getElementById('sku-filter-options');
    const searchEl = document.getElementById('sku-filter-search');
    if (!optionsEl) return;

    const options = getSkuOptionList();
    const allValues = options.map(opt => opt.value);

    ensureSkuFilterTemp();

    const query = (searchEl ? searchEl.value : '').trim().toLowerCase();
    const filtered = options.filter(opt => {
        if (!query) return true;
        return opt.label.toLowerCase().includes(query);
    });

    const allSelected = allValues.length > 0 && allValues.every(val => skuFilterTemp.has(val));
    const selectAllRow = `
        <div class="preset-filter-option">
            <label>
                <input type="checkbox" data-value="__all__" ${allSelected ? 'checked' : ''}>
                <span>Select all</span>
            </label>
            <span class="preset-filter-option-count">${allValues.length}/${allValues.length}</span>
        </div>
    `;

    const rows = filtered.map(opt => `
        <div class="preset-filter-option">
            <label>
                <input type="checkbox" data-value="${escapeHtml(opt.value)}" ${skuFilterTemp.has(opt.value) ? 'checked' : ''}>
                <span>${escapeHtml(opt.label)}</span>
            </label>
            <span class="preset-filter-option-count">${opt.count}/${opt.total}</span>
        </div>
    `).join('');

    optionsEl.innerHTML = selectAllRow + rows;
}

function initSkuFilter() {
    const menu = document.getElementById('sku-filter-menu');
    const toggle = document.getElementById('sku-filter-toggle');
    const okBtn = document.getElementById('sku-filter-ok');
    const cancelBtn = document.getElementById('sku-filter-cancel');
    const optionsEl = document.getElementById('sku-filter-options');
    const searchEl = document.getElementById('sku-filter-search');

    if (toggle && !toggle.hasAttribute('data-listener-attached')) {
        toggle.setAttribute('data-listener-attached', '1');
        toggle.addEventListener('click', toggleSkuFilter);
    }
    if (okBtn && !okBtn.hasAttribute('data-listener-attached')) {
        okBtn.setAttribute('data-listener-attached', '1');
        okBtn.addEventListener('click', function(e) {
            e.preventDefault();
            closeSkuFilter(true);
        });
    }
    if (cancelBtn && !cancelBtn.hasAttribute('data-listener-attached')) {
        cancelBtn.setAttribute('data-listener-attached', '1');
        cancelBtn.addEventListener('click', function(e) {
            e.preventDefault();
            closeSkuFilter(false);
        });
    }
    if (optionsEl && !optionsEl.hasAttribute('data-listener-attached')) {
        optionsEl.setAttribute('data-listener-attached', '1');
        optionsEl.addEventListener('change', function(e) {
            const checkbox = e.target;
            if (!checkbox || !checkbox.dataset || !checkbox.dataset.value) return;
            const value = checkbox.dataset.value;
            ensureSkuFilterTemp();
            if (value === '__all__') {
                const options = getSkuOptionList();
                const allValues = options.map(opt => opt.value);
                if (checkbox.checked) {
                    skuFilterTemp = new Set(allValues);
                } else {
                    skuFilterTemp.clear();
                }
                renderSkuFilterOptions();
                return;
            }
            if (checkbox.checked) {
                skuFilterTemp.add(value);
            } else {
                skuFilterTemp.delete(value);
            }
            renderSkuFilterOptions();
        });
    }
    if (searchEl && !searchEl.hasAttribute('data-listener-attached')) {
        searchEl.setAttribute('data-listener-attached', '1');
        searchEl.addEventListener('input', function() {
            renderSkuFilterOptions();
        });
    }

    updateSkuFilterCount();

    if (!skuFilterInitDone) {
        skuFilterInitDone = true;
        document.addEventListener('click', function(e) {
            if (!skuFilterOpen) return;
            const filterCell = document.getElementById('sku-filter-cell');
            if (filterCell && filterCell.contains(e.target)) {
                return;
            }
            closeSkuFilter(false);
        });
    }
}

function getFilteredItems(sourceItems) {
    let filtered = sourceItems;

    if (presetFilterSelected) {
        filtered = filtered.filter(item => {
            const name = (item.preset_name || '').trim();
            const value = name ? name : PRESET_BLANK;
            if (presetFilterStickyItems.has(item.item_name)) {
                return true;
            }
            return presetFilterSelected.has(value);
        });
    }

    if (cancelledFilterSelected) {
        filtered = filtered.filter(item => {
            const status = (item.cancelled_status || '').trim();
            const value = status === STATUS_CANCELLED
                ? STATUS_CANCELLED
                : status === STATUS_FAILED
                    ? STATUS_FAILED
                    : STATUS_PROCESSED;
            return cancelledFilterSelected.has(value);
        });
    }

    if (skuFilterSelected) {
        filtered = filtered.filter(item => {
            const sku = (item.sku || '').trim();
            const value = sku ? sku : SKU_BLANK;
            if (skuFilterStickyItems.has(item.item_name)) {
                return true;
            }
            return skuFilterSelected.has(value);
        });
    }

    if (aiMatchOnly) {
        filtered = filtered.filter(item => aiResultsByItem.has(item.item_name));
    }

    // Search filter
    if (searchQuery) {
        const q = searchQuery.toLowerCase();
        filtered = filtered.filter(item => {
            return (item.item_name || '').toLowerCase().includes(q) ||
                   (item.preset_name || '').toLowerCase().includes(q) ||
                   (item.sku || '').toLowerCase().includes(q) ||
                   (item.buyer || '').toLowerCase().includes(q) ||
                   (item.notes || '').toLowerCase().includes(q) ||
                   (item.order_id || '').toLowerCase().includes(q) ||
                   (item.pinned_message || '').toLowerCase().includes(q) ||
                   (item.sold_price || '').toLowerCase().includes(q);
        });
    }

    return filtered;
}

function maybeAddSkuSticky(itemName) {
    if (!itemName) return;
    if (skuFilterSelected) {
        skuFilterStickyItems.add(itemName);
    }
}

function maybeAddPresetSticky(itemName) {
    if (!itemName) return;
    if (presetFilterSelected) {
        presetFilterStickyItems.add(itemName);
    }
}

function getColumnFillCounts(sourceItems) {
    const total = sourceItems.length;
    const counts = {};

    const isFilled = (val) => val !== null && val !== undefined && String(val).trim() !== '';

    sourceItems.forEach(item => {
        if (isFilled(item.item_name)) counts.item_name = (counts.item_name || 0) + 1;
        if (isFilled(item.sold_timestamp)) counts.sold_timestamp = (counts.sold_timestamp || 0) + 1;
        if (isFilled(item.viewers)) counts.viewers = (counts.viewers || 0) + 1;
        if (isFilled(item.order_id)) counts.order_id = (counts.order_id || 0) + 1;
        if (isFilled(item.buyer)) counts.buyer = (counts.buyer || 0) + 1;
        if (isFilled(item.cancelled_status)) counts.cancelled_status = (counts.cancelled_status || 0) + 1;
        if (isFilled(item.sku)) counts.sku = (counts.sku || 0) + 1;
        if (isFilled(item.notes)) counts.notes = (counts.notes || 0) + 1;
        if (isFilled(item.preset_name)) counts.preset = (counts.preset || 0) + 1;
        if (isFilled(item.pinned_message)) counts.pinned_message = (counts.pinned_message || 0) + 1;
        if (isFilled(item.sold_price) || item.sold_price_float !== null) counts.sold_price = (counts.sold_price || 0) + 1;
        if (item.sold_price_float !== null && item.sold_price_float !== undefined) counts.net_revenue = (counts.net_revenue || 0) + 1;
        if (item.cost !== null && item.cost !== undefined) counts.cost = (counts.cost || 0) + 1;
        if ((item.sold_price_float !== null && item.sold_price_float !== undefined) && (item.cost !== null && item.cost !== undefined)) {
            counts.profit = (counts.profit || 0) + 1;
        }
        if (isFilled(item.image)) counts.image = (counts.image || 0) + 1;
    });

    return { counts, total };
}

function renderFilterRow(visibleColumns) {
    return '';
}

// Helper function to render header cell based on column ID
function renderHeaderCell(colId) {
    const config = columnConfig.find(col => col.id === colId);
    if (!config) return '';

    const colClass = `col-${colId}`;
    if (colId === 'row_number') {
        return `<th class="checkbox-col ${colClass}">#<div class="resize-border"></div></th>`;
    } else if (colId === 'image') {
        return `<th class="image-col ${colClass}">Image<div class="resize-border"></div></th>`;
    } else if (colId === 'net_revenue') {
        const sortField = config.field || colId;
        return `<th class="${colClass} sortable-header" data-sort-field="${sortField}">
            Net Revenue
            <span class="tooltip tooltip-inline">
                ⓘ
                <span class="tooltiptext">Net Revenue = Sold Price × (1 - Commission Rate - 0.029) - 0.30</span>
            </span>
            <span class="sort-indicator" id="sort-${sortField}"></span>
            <div class="resize-border"></div>
        </th>`;
    } else if (config.sortable) {
        const sortField = config.field || colId;
        return `<th class="${colClass}${colId === 'item_name' ? ' item-name-col' : ''} sortable-header" data-sort-field="${sortField}">${config.label}<span class="sort-indicator" id="sort-${sortField}"></span><div class="resize-border"></div></th>`;
    } else {
        return `<th class="${colClass}">${config.label}<div class="resize-border"></div></th>`;
    }
}

// Helper function to render data cell based on column ID
function renderDataCell(colId, item, index, commissionRate, presetsForTemplate) {
    const config = columnConfig.find(col => col.id === colId);
    if (!config) return '';
    
    if (colId === 'row_number') {
        return `<td class="row-number" data-index="${index}">${index + 1}</td>`;
    } else if (colId === 'item_name') {
        return `<td>${escapeHtml(item.item_name)}</td>`;
    } else if (colId === 'sold_timestamp') {
        return `<td contenteditable="true" 
                class="editable-cell editable-sold-time cell-min-120" 
                data-item-name="${escapeHtml(item.item_name)}"
                data-item-key="${getItemKey(item.item_name)}"
                data-field="sold_timestamp"
                >${escapeHtml(item.sold_timestamp || '')}</td>`;
    } else if (colId === 'viewers') {
        return `<td contenteditable="true" 
                class="editable-cell editable-viewers cell-min-80" 
                data-item-name="${escapeHtml(item.item_name)}"
                data-item-key="${getItemKey(item.item_name)}"
                data-field="viewers"
                >${escapeHtml(item.viewers || '')}</td>`;
    } else if (colId === 'order_id') {
        return `<td contenteditable="true" 
                class="editable-cell editable-order-id cell-min-90" 
                data-item-name="${escapeHtml(item.item_name)}"
                data-item-key="${getItemKey(item.item_name)}"
                data-field="order_id"
                >${escapeHtml(item.order_id || '')}</td>`;
    } else if (colId === 'buyer') {
        return `<td contenteditable="true" 
                class="editable-cell editable-buyer cell-min-120" 
                data-item-name="${escapeHtml(item.item_name)}"
                data-item-key="${getItemKey(item.item_name)}"
                data-field="buyer"
                >${escapeHtml(item.buyer || '')}</td>`;
    } else if (colId === 'cancelled_status') {
        const statusValue = item.cancelled_status || '';
        return `<td>
                <select class="cancelled-status-select" 
                        data-item-name="${escapeHtml(item.item_name)}"
                        data-field="cancelled_status">
                    <option value="${STATUS_PROCESSED}" ${statusValue === '' ? 'selected' : ''}>Processed</option>
                    <option value="Cancelled" ${statusValue === 'Cancelled' ? 'selected' : ''}>Cancelled</option>
                    <option value="Failed" ${statusValue === 'Failed' ? 'selected' : ''}>Failed</option>
                </select>
            </td>`;
    } else if (colId === 'sku') {
        const skuValue = item.sku || '';
        const aiSuggestion = getAiSuggestion(item.item_name);
        const aiSku = aiSuggestion && aiSuggestion.sku ? aiSuggestion.sku : '';
        const aiSkuBadge = aiSku && !skuValue && !isAiDismissed(item.item_name, 'sku')
            ? `<div class="ai-badge" title="AI suggested SKU">
                    <span>AI: ${escapeHtml(aiSku)} (${formatAiConfidence(aiSuggestion.confidence)})</span>
                    <button type="button" class="ai-dismiss-btn" onclick="dismissAiSuggestion('${escapeHtml(item.item_name)}', 'sku')">✕</button>
                </div>`
            : '';
        return `<td class="cell-min-100 sku-cell"
                data-item-name="${escapeHtml(item.item_name)}"
                data-item-key="${getItemKey(item.item_name)}"
                data-field="sku">
                <div class="sku-text editable-cell"
                     contenteditable="true"
                     data-index="${index}"
                     data-item-name="${escapeHtml(item.item_name)}"
                     data-item-key="${getItemKey(item.item_name)}"
                     data-field="sku">${escapeHtml(skuValue)}</div>
                <div class="sku-select-row">
                    <select class="sku-select"
                            data-index="${index}"
                            data-item-name="${escapeHtml(item.item_name)}"
                            data-item-key="${getItemKey(item.item_name)}">
                        ${getSkuOptionsMarkup(skuValue)}
                    </select>
                </div>
                ${aiIncludeSku ? aiSkuBadge : ''}
            </td>`;
    } else if (colId === 'notes') {
        return `<td contenteditable="true" 
                class="editable-cell editable-notes cell-min-150" 
                data-item-name="${escapeHtml(item.item_name)}"
                data-item-key="${getItemKey(item.item_name)}"
                data-field="notes"
                >${escapeHtml(item.notes || '')}</td>`;
    } else if (colId === 'preset') {
        const aiSuggestion = getAiSuggestion(item.item_name);
        const aiPreset = aiSuggestion && aiSuggestion.preset_name ? aiSuggestion.preset_name : '';
        const aiPresetBadge = aiPreset && !item.preset_name && !isAiDismissed(item.item_name, 'preset')
            ? `<div class="ai-badge" title="AI suggested preset">
                    <span>AI: ${escapeHtml(aiPreset)} (${formatAiConfidence(aiSuggestion.confidence)})</span>
                    <button type="button" class="ai-dismiss-btn" onclick="dismissAiSuggestion('${escapeHtml(item.item_name)}', 'preset')">✕</button>
                </div>`
            : '';
        const giveawayTag = isGiveawayPreset(item.preset_name)
            ? `<div class="ai-badge" title="Giveaway preset">Giveaway</div>`
            : '';
        return `<td class="preset-cell editable-preset-cell" 
                contenteditable="false"
                data-index="${index}"
                data-item-name="${escapeHtml(item.item_name)}"
                data-field="preset">
                <div class="preset-wrapper">
                    <div class="preset-display-row">
                        <div class="preset-top">
                            <div class="preset-text editable-cell" 
                                 contenteditable="true"
                                 data-index="${index}"
                                 data-item-name="${escapeHtml(item.item_name)}"
                                 data-field="preset_display">${escapeHtml(item.preset_name || '')}</div>
                        </div>
                    </div>
                    <div class="preset-select-row">
                        <button type="button"
                                class="preset-select-toggle"
                                data-index="${index}"
                                data-item-name="${escapeHtml(item.item_name)}"
                                data-value="${escapeHtml(item.preset_name || '')}">
                            ${item.preset_name ? escapeHtml(item.preset_name) : 'Select preset...'}
                        </button>
                        <div class="preset-select-menu preset-filter-menu">
                            <div class="preset-filter-search">
                                <input type="text" class="preset-select-search" placeholder="Search presets...">
                            </div>
                            <div class="preset-select-options preset-filter-options">
                                <div class="preset-filter-option preset-select-option" data-value="">
                                    <label>
                                        <input type="checkbox" data-value="">
                                        <span>Select preset...</span>
                                    </label>
                                </div>
                                ${presetsForTemplate.map(preset => {
                                    const optionValue = `${preset.name}|${preset.cost}`;
                                    return `<div class="preset-filter-option preset-select-option" data-value="${escapeHtml(optionValue)}">
                                        <label>
                                            <input type="checkbox" data-value="${escapeHtml(optionValue)}">
                                            <span>${escapeHtml(preset.name)} ($${preset.cost.toFixed(2)})</span>
                                        </label>
                                    </div>`;
                                }).join('')}
                            </div>
                            <div class="preset-filter-actions">
                                <button class="btn btn-secondary btn-small preset-select-cancel" type="button">Cancel</button>
                                <button class="btn btn-success btn-small preset-select-ok" type="button">OK</button>
                            </div>
                        </div>
                    </div>
                    <div class="preset-controls preset-input-row">
                    <input type="text" 
                           class="item-preset-name" 
                           data-index="${index}"
                               placeholder="Preset name">
                    <input type="number" 
                           class="item-preset-cost" 
                           data-index="${index}"
                           placeholder="Cost"
                               step="0.01">
                    <button class="save-preset-btn" 
                            data-index="${index}"
                                onclick="savePresetFromRow(${index})">Save</button>
                    </div>
                    <div class="preset-bottom-spacer"></div>
                </div>
                ${aiPresetBadge}
                ${giveawayTag}
            </td>`;
    } else if (colId === 'pinned_message') {
        return `<td contenteditable="true" 
                class="editable-cell editable-pinned-message cell-min-100" 
                data-item-name="${escapeHtml(item.item_name)}"
                data-item-key="${getItemKey(item.item_name)}"
                data-field="pinned_message"
                >${escapeHtml(item.pinned_message || '')}</td>`;
    } else if (colId === 'sold_price') {
        return `<td contenteditable="true" 
                class="editable-cell sold-price editable-sold-price cell-min-80" 
                data-item-name="${escapeHtml(item.item_name)}"
                data-item-key="${getItemKey(item.item_name)}"
                data-field="sold_price"
                >${escapeHtml(item.sold_price || '')}</td>`;
    } else if (colId === 'net_revenue') {
        const netRevenue = calcNetRevenue(item.sold_price_float, item.preset_name);
        return `<td contenteditable="true"
                class="editable-cell net-revenue editable-net-revenue cell-min-80"
                data-item-name="${escapeHtml(item.item_name)}"
                data-item-key="${getItemKey(item.item_name)}"
                data-field="net_revenue"
                >${netRevenue !== null ? '$' + netRevenue.toFixed(2) : 'N/A'}</td>`;
    } else if (colId === 'cost') {
        const costValue = parseNumber(item.cost);
        return `<td contenteditable="true"
                class="editable-cell cost editable-cost cell-min-80"
                data-item-name="${escapeHtml(item.item_name)}"
                data-item-key="${getItemKey(item.item_name)}"
                data-field="cost"
                >${costValue !== null ? costValue.toFixed(2) : ''}</td>`;
    } else if (colId === 'profit') {
        const profit = calcProfit(item.sold_price_float, item.cost, item.preset_name);
        const profitClass = profit !== null ? (profit >= 0 ? 'profit-positive' : 'profit-negative') : '';
        return `<td contenteditable="false" 
                class="profit ${profitClass} cell-min-80">${profit !== null ? '$' + profit.toFixed(2) : 'N/A'}</td>`;
    } else if (colId === 'image') {
        return `<td class="image-cell" 
                data-item-name="${escapeHtml(item.item_name)}"
                data-item-key="${getItemKey(item.item_name)}"
                data-field="image">
                ${item.image ? 
                    `<img src="${item.image}" class="image-thumb" onclick="handleImageClick(event, '${item.image}')" alt="Item image">` : 
                    '<div class="image-placeholder">Paste image or URL</div>'
                }
            </td>`;
    }
    return '';
}

// Differential update for recording polls - only modifies changed rows
function diffUpdateTable() {
    const tbody = document.getElementById('items-table-body');
    if (!tbody) return renderTable();

    applySorting();
    const visibleColumns = getVisibleColumns();
    const itemsToRender = getFilteredItems(items);
    displayItems = itemsToRender;
    const commissionRate = parseFloat(document.getElementById('commission-rate').value) / 100;
    const presetsForTemplate = presets;

    // Get existing data rows (skip totals row at index 0)
    const existingRows = Array.from(tbody.querySelectorAll('tr:not(.totals-row)'));
    const existingKeys = existingRows.map(tr => tr.getAttribute('data-item-key'));

    // Build map of new items
    const newKeys = itemsToRender.map(item => item.item_name);

    // Find new rows to append (items not in existing DOM)
    const existingKeySet = new Set(existingKeys);
    let addedCount = 0;
    for (let i = 0; i < itemsToRender.length; i++) {
        const item = itemsToRender[i];
        if (!existingKeySet.has(item.item_name)) {
            // New item - append row
            const cells = visibleColumns.map(colId => renderDataCell(colId, item, i, commissionRate, presetsForTemplate)).join('');
            const tr = document.createElement('tr');
            tr.setAttribute('data-item-key', item.item_name);
            tr.innerHTML = cells;
            tbody.appendChild(tr);
            addedCount++;
        }
    }

    // Update row numbers and calculated fields for existing rows
    const newKeySet = new Set(newKeys);
    existingRows.forEach(tr => {
        const key = tr.getAttribute('data-item-key');
        if (!key) return;
        if (!newKeySet.has(key)) {
            // Item removed - remove row
            tr.remove();
            return;
        }
        // Update row number
        const idx = newKeys.indexOf(key);
        const rowNumCell = tr.querySelector('.row-number');
        if (rowNumCell) rowNumCell.textContent = idx + 1;

        // Update calculated fields (net revenue, profit) in place
        const item = itemsToRender[idx];
        if (!item) return;
        const netRevCell = tr.querySelector('.net-revenue');
        if (netRevCell) {
            const netRev = calcNetRevenue(item.sold_price_float, item.preset_name);
            netRevCell.textContent = netRev !== null ? '$' + netRev.toFixed(2) : 'N/A';
        }
        const profitCell = tr.querySelector('.profit');
        if (profitCell) {
            const profit = calcProfit(item.sold_price_float, item.cost, item.preset_name);
            profitCell.textContent = profit !== null ? '$' + profit.toFixed(2) : 'N/A';
            profitCell.className = 'profit cell-min-80 ' + (profit !== null ? (profit >= 0 ? 'profit-positive' : 'profit-negative') : '');
        }
    });

    if (addedCount > 0) {
        // Re-attach listeners only for new rows
        attachEditableListeners();
        addClickModeIcons();
        scrubClickModeText();
        // Attach row number click handlers for new rows
        tbody.querySelectorAll('.row-number').forEach((rowNum) => {
            if (rowNum.hasAttribute('data-diff-init')) return;
            rowNum.setAttribute('data-diff-init', '1');
            rowNum.addEventListener('click', function(e) {
                e.stopPropagation();
                lastRowNumberClickAt = Date.now();
                const index = parseInt(this.getAttribute('data-index'));
                if (e.shiftKey && lastSelectedRowIndex !== null && lastSelectedRowIndex !== index) {
                    selectRowRange(lastSelectedRowIndex, index);
                } else if (e.metaKey || e.ctrlKey) {
                    toggleRow(index);
                } else {
                    selectRow(index, false);
                }
            });
        });
    }

    // Always update totals
    updateTotals();
    updateTfootTotals();
}

function renderTable() {
    resetColumnInteractions();
    resetColumnInteractionState();
    // Apply sorting before rendering
    applySorting();
    
    // Get visible columns (exclude hidden ones)
    const visibleColumns = getVisibleColumns();
    
    // Update table headers based on visible columns
    const thead = document.querySelector('thead');
    if (thead) {
        const headerRow = `<tr class="header-row">${visibleColumns.map(colId => renderHeaderCell(colId)).join('')}</tr>`;
        thead.innerHTML = headerRow;
    }
    
    const tbody = document.getElementById('items-table-body');
    const itemsToRender = getFilteredItems(items);
    displayItems = itemsToRender;
    
    // Get commission rate (convert from percentage to decimal)
    const commissionRate = parseFloat(document.getElementById('commission-rate').value) / 100;
    
    // Make presets available in template scope
    const presetsForTemplate = presets;
    
    // Build table rows with totals row at top
    // Find indices for totals columns (in visible columns)
    const netRevenueIndex = visibleColumns.indexOf('net_revenue');
    const costIndex = visibleColumns.indexOf('cost');
    const profitIndex = visibleColumns.indexOf('profit');
    const soldPriceIndex = visibleColumns.indexOf('sold_price');
    
    const fillInfo = getColumnFillCounts(items);
    const totalCount = fillInfo.total || 0;
    const totalsRowTop = `<tr class="totals-row" id="totals-row-top">
        ${visibleColumns.map(colId => {
            if (colId === 'preset') {
                return `<td class="cell-muted">
                    <div class="preset-filter" id="preset-filter-cell">
                        <button type="button" class="preset-filter-btn" id="preset-filter-toggle">Filter ▾</button>
                        <span class="preset-filter-count" id="preset-filter-count">0/0</span>
                        <div class="preset-filter-menu" id="preset-filter-menu">
                            <div class="preset-filter-search">
                                <input type="text" id="preset-filter-search" placeholder="Search presets">
                            </div>
                            <div class="preset-filter-options" id="preset-filter-options"></div>
                            <div class="preset-filter-actions">
                                <button class="btn btn-secondary btn-small" id="preset-filter-cancel">Cancel</button>
                                <button class="btn btn-success btn-small" id="preset-filter-ok">OK</button>
                            </div>
                        </div>
                    </div>
                </td>`;
            }
            if (colId === 'sku') {
                return `<td class="cell-muted">
                    <div class="preset-filter" id="sku-filter-cell">
                        <button type="button" class="preset-filter-btn" id="sku-filter-toggle">Filter ▾</button>
                        <span class="preset-filter-count" id="sku-filter-count">0/0</span>
                        <div class="preset-filter-menu" id="sku-filter-menu">
                            <div class="preset-filter-search">
                                <input type="text" id="sku-filter-search" placeholder="Search SKUs">
                            </div>
                            <div class="preset-filter-options" id="sku-filter-options"></div>
                            <div class="preset-filter-actions">
                                <button class="btn btn-secondary btn-small" id="sku-filter-cancel">Cancel</button>
                                <button class="btn btn-success btn-small" id="sku-filter-ok">OK</button>
                            </div>
                        </div>
                    </div>
                </td>`;
            }
            if (colId === 'cancelled_status') {
                return `<td class="cell-muted">
                    <div class="preset-filter" id="cancelled-filter-cell">
                        <button type="button" class="preset-filter-btn" id="cancelled-filter-toggle">Filter ▾</button>
                        <span class="preset-filter-count" id="cancelled-filter-count">0/0</span>
                        <div class="preset-filter-menu" id="cancelled-filter-menu">
                            <div class="preset-filter-search">
                                <input type="text" id="cancelled-filter-search" placeholder="Search status">
                            </div>
                            <div class="preset-filter-options" id="cancelled-filter-options"></div>
                            <div class="preset-filter-actions">
                                <button class="btn btn-secondary btn-small" id="cancelled-filter-cancel">Cancel</button>
                                <button class="btn btn-success btn-small" id="cancelled-filter-ok">OK</button>
                            </div>
                        </div>
                    </div>
                </td>`;
            }
            if (!totalCount) return '<td></td>';
            if (colId === 'row_number') return '<td></td>';
            const count = fillInfo.counts[colId] || 0;
            return `<td class="cell-muted">${count}/${totalCount}</td>`;
        }).join('')}
    </tr>`;
    
    const dataRows = itemsToRender.map((item, index) => {
        const cells = visibleColumns.map(colId => renderDataCell(colId, item, index, commissionRate, presetsForTemplate)).join('');
        return `<tr data-item-key="${escapeHtml(item.item_name)}">${cells}</tr>`;
    }).join('');

    if (itemsToRender.length === 0) {
        const message = items.length > 0
            ? 'No items match the filters.'
            : 'No items found. Run monitor.py to capture items.';
        tbody.innerHTML = totalsRowTop + `<tr><td colspan="${visibleColumns.length}">${message}</td></tr>`;
    } else {
        tbody.innerHTML = totalsRowTop + dataRows;
    }
    
    // Reapply column widths after render
    applyColumnWidths();
    
    // Attach event listeners to editable inputs
    attachEditableListeners();
    
    // Add Click Mode icons
    addClickModeIcons();
    scrubClickModeText();

    // Reattach column resize handlers after render
    initColumnResize();

    initPresetSelectSearch();

    // Reapply any pending edits after render
    pendingEdits.forEach((value, key) => {
        const [itemKey, field] = key.split('::');
        const cell = document.querySelector(
            `.editable-cell[data-item-key="${itemKey}"][data-field="${field}"]`
        );
        if (cell) {
            cell.textContent = value;
        }
    });
    
    // Attach row number click handlers
    document.querySelectorAll('.row-number').forEach((rowNum) => {
        rowNum.addEventListener('click', function(e) {
            e.stopPropagation();
            lastRowNumberClickAt = Date.now();
            const index = parseInt(this.getAttribute('data-index'));
            if (e.shiftKey && lastSelectedRowIndex !== null && lastSelectedRowIndex !== index) {
                // Range selection
                selectRowRange(lastSelectedRowIndex, index);
            } else if (e.metaKey || e.ctrlKey) {
                // Add to selection (multi-select)
                toggleRow(index);
            } else {
                // Single selection
                selectRow(index, false);
            }
        });
    });
    
    // Update totals
    updateTotals();
    
    // Add event listener to commission rate input to recalculate on change
    const commissionRateInput = document.getElementById('commission-rate');
    if (commissionRateInput && !commissionRateInput.hasAttribute('data-listener-attached')) {
        commissionRateInput.setAttribute('data-listener-attached', 'true');
        commissionRateInput.addEventListener('change', () => {
            renderTable();
        });
        commissionRateInput.addEventListener('input', () => {
            renderTable();
        });
    }
    
    updateSelectedCount();
    
    // Update sort indicators
    document.querySelectorAll('.sort-indicator').forEach(indicator => {
        indicator.textContent = '';
        indicator.classList.remove('active');
    });
    
    if (sortColumn) {
        const indicator = document.getElementById(`sort-${sortColumn}`);
        if (indicator) {
            indicator.textContent = sortDirection === 'asc' ? '▲' : '▼';
            indicator.classList.add('active');
        }
    }
    
    // Update footer totals row
    updateTfootTotals();
    
    // Update column visibility panel
    updateColumnVisibilityMenu();

    // Initialize preset filter UI
    initPresetFilter();
    
    // Initialize cancelled/failed filter UI
    initCancelledFilter();

    // Initialize SKU filter UI
    initSkuFilter();
    
    // Initialize column reordering after render
    initColumnReorder();
    updateDebugPanel();
}

function resetColumnInteractionState() {
    if (typeof columnResizeState !== 'undefined') {
        columnResizeState.active = false;
        columnResizeState.pending = false;
        columnResizeState.moved = false;
        if (columnResizeState.header) {
            columnResizeState.header.classList.remove('resizing');
        }
        columnResizeState.header = null;
        columnResizeState.columnId = null;
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
    }
    if (typeof columnDragState !== 'undefined') {
        columnDragState.active = false;
        columnDragState.pending = false;
        columnDragState.moved = false;
        columnDragState.lastDragAt = 0;
        if (columnDragState.headers && columnDragState.headers.length) {
            columnDragState.headers.forEach(h => h.classList.remove('drag-over', 'dragging'));
        }
        columnDragState.headers = [];
        columnDragState.draggedIndex = -1;
        columnDragState.draggedColId = null;
    }
}

function clearColumnInteractionsIfIdle() {
    if (typeof columnResizeState !== 'undefined' && columnResizeState.pending && !columnResizeState.active) {
        columnResizeState.pending = false;
        columnResizeState.moved = false;
        columnResizeState.header = null;
        columnResizeState.columnId = null;
    }
    if (typeof columnDragState !== 'undefined' && columnDragState.pending && !columnDragState.active) {
        columnDragState.pending = false;
        columnDragState.moved = false;
        columnDragState.headers = [];
        columnDragState.draggedIndex = -1;
        columnDragState.draggedColId = null;
        columnDragState.lastDragAt = 0;
    }
}

function setDebugEvent(message) {
    if (!debugEnabled) return;
    debugLastEvent = message;
    updateDebugPanel();
}

function updateDebugPanel() {
    const panel = document.getElementById('debug-panel');
    if (!panel) return;
    panel.style.display = debugEnabled ? 'block' : 'none';
    if (!debugEnabled) return;
    const sortEl = document.getElementById('debug-sort-state');
    const resizeEl = document.getElementById('debug-resize-state');
    const dragEl = document.getElementById('debug-drag-state');
    const lastEl = document.getElementById('debug-last-event');
    if (sortEl) {
        sortEl.textContent = `Sort: ${sortColumn || 'none'} (${sortDirection})`;
    }
    if (resizeEl && typeof columnResizeState !== 'undefined') {
        resizeEl.textContent = `Resize: pending=${!!columnResizeState.pending}, active=${!!columnResizeState.active}, moved=${!!columnResizeState.moved}`;
    }
    if (dragEl && typeof columnDragState !== 'undefined') {
        dragEl.textContent = `Drag: pending=${!!columnDragState.pending}, active=${!!columnDragState.active}, moved=${!!columnDragState.moved}`;
    }
    if (lastEl) {
        lastEl.textContent = `Last: ${debugLastEvent || 'none'}`;
    }
}

function initPresetSelectSearch() {
    document.querySelectorAll('.preset-select-row').forEach(row => {
        if (row.hasAttribute('data-listener-attached')) return;
        row.setAttribute('data-listener-attached', '1');
        const toggle = row.querySelector('.preset-select-toggle');
        const menu = row.querySelector('.preset-select-menu');
        const search = row.querySelector('.preset-select-search');
        const optionsWrap = row.querySelector('.preset-select-options');
        const okBtn = row.querySelector('.preset-select-ok');
        const cancelBtn = row.querySelector('.preset-select-cancel');
        if (!toggle || !menu || !search || !optionsWrap) return;
        let pendingValue = toggle.getAttribute('data-value') || '';

        toggle.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            document.querySelectorAll('.preset-select-menu.open').forEach(openMenu => {
                if (openMenu !== menu) openMenu.classList.remove('open');
            });
            menu.classList.toggle('open');
            if (menu.classList.contains('open')) {
                search.value = '';
                Array.from(optionsWrap.children).forEach(opt => {
                    opt.style.display = '';
                    const input = opt.querySelector('input[type="checkbox"]');
                    if (input) {
                        input.checked = input.dataset.value === pendingValue;
                    }
                });
                search.focus();
            }
        });

        search.addEventListener('input', function() {
            const query = (search.value || '').trim().toLowerCase();
            Array.from(optionsWrap.children).forEach(opt => {
                const text = (opt.textContent || '').toLowerCase();
                opt.style.display = query && !text.includes(query) ? 'none' : '';
            });
        });

        optionsWrap.addEventListener('click', function(e) {
            const option = e.target.closest('.preset-select-option');
            if (!option) return;
            const value = option.dataset.value || '';
            pendingValue = value;
            Array.from(optionsWrap.querySelectorAll('input[type="checkbox"]')).forEach(cb => {
                cb.checked = cb.dataset.value === pendingValue;
            });
        });

        if (okBtn) {
            okBtn.addEventListener('click', function() {
                menu.classList.remove('open');
                if (!pendingValue) {
                    toggle.textContent = 'Select preset...';
                    toggle.setAttribute('data-value', '');
                    return;
                }
                const index = parseInt(toggle.getAttribute('data-index'), 10);
                handlePresetSelect(index, pendingValue);
                const selectedOption = optionsWrap.querySelector(`.preset-select-option[data-value="${CSS.escape(pendingValue)}"] span`);
                toggle.textContent = selectedOption ? selectedOption.textContent.trim() : 'Select preset...';
                toggle.setAttribute('data-value', pendingValue);
            });
        }

        if (cancelBtn) {
            cancelBtn.addEventListener('click', function() {
                menu.classList.remove('open');
            });
        }
    });

    if (!window.__presetSelectDocClickBound) {
        window.__presetSelectDocClickBound = true;
        document.addEventListener('click', function(e) {
            if (e.target.closest('.preset-select-row')) return;
            document.querySelectorAll('.preset-select-menu.open').forEach(menu => {
                menu.classList.remove('open');
            });
        });
    }
}

function bindSortHandlers() {
    document.querySelectorAll('thead tr.header-row th.sortable-header').forEach(th => {
        th.onclick = function(e) {
            clearColumnInteractionsIfIdle();
            if (e.target.closest('.resize-border')) return;
            if (Date.now() < sortClickLockUntil) return;
            if (columnResizeState.pending || columnResizeState.active) return;
            if (columnDragState.active) return;
            if (columnDragState.moved) return;
            const sortField = th.dataset.sortField;
            setDebugEvent(`headerClick(${sortField || 'none'})`);
            if (sortField) sortTable(sortField);
        };
    });
}

function initGlobalHeaderSort() {
    if (window.__headerSortBound) return;
    window.__headerSortBound = true;
    document.addEventListener('click', function(e) {
        const header = e.target.closest('thead tr.header-row th.sortable-header');
        if (!header) return;
        clearColumnInteractionsIfIdle();
        if (e.target.closest('.resize-border')) return;
        if (Date.now() < sortClickLockUntil) return;
        if (columnResizeState.pending || columnResizeState.active) return;
        if (columnDragState.active) return;
        const sortField = header.dataset.sortField;
        setDebugEvent(`globalHeaderClick(${sortField || 'none'})`);
        if (sortField) sortTable(sortField);
    }, true);
}

function stripClickModeToken(text) {
    if (!text) return '';
    return text
        .replace(/\bCM\b/g, '')
        .replace(/([A-Za-z0-9])CM\b/g, '$1')
        .replace(/\s{2,}/g, ' ')
        .trim();
}

async function saveSkuValue({ itemName, itemKey, value, index, element }) {
    if (!itemName) return;

    // Save state for undo
    saveState();

    try {
        const response = await fetch('/api/items/sku', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                item_name: itemName,
                sku: value || null,
                show_id: currentShowId
            })
        });
        if (!response.ok) {
            const text = await response.text();
            let message = 'Unknown error';
            try {
                const parsed = JSON.parse(text);
                message = parsed.error || message;
            } catch (e) {
                if (text) message = text;
            }
            alert('Error updating SKU: ' + message);
            await loadData();
            return;
        }

        const item = items.find(i => i.item_name === itemName);
        if (item) {
            item.sku = value || null;
            if (value && item.preset_name) {
                skuPresetMap.set(value.toLowerCase(), item.preset_name);
            }
            maybeAddSkuSticky(item.item_name);
            learnAiFromItem(item.item_name);
        }
        if (itemKey) {
            pendingEdits.delete(`${itemKey}::sku`);
        }

        const mappedPreset = value ? skuPresetMap.get(value.toLowerCase()) : null;
        if (mappedPreset && !isNaN(index)) {
            const preset = presets.find(p => p.name === mappedPreset);
            if (preset && item && item.preset_name !== preset.name) {
                await applyPresetToRow(index, preset.name, preset.cost);
            }
        }
    } catch (error) {
        console.error('Error updating SKU:', error);
        alert('Error updating SKU: ' + error.message);
        await loadData();
    } finally {
        if (element) {
            element.setAttribute('data-last-saved', value || '');
        }
    }
}

function attachContenteditableCellListeners() {
    // Handle contenteditable cells (spreadsheet-style)
    const autosaveFields = new Set(['sku', 'notes', 'pinned_message']);
    const autosaveTimers = new WeakMap();

    document.querySelectorAll('.editable-cell[contenteditable="true"]').forEach(cell => {
        // Store original blur handler
        const originalBlur = cell.onblur;
        const field = cell.getAttribute('data-field');

        cell.addEventListener('mousedown', function() {
            // Single click should replace on type
            this.dataset.replaceOnType = 'true';
        });

        cell.addEventListener('dblclick', function() {
            // Double click should edit at caret without replace
            this.dataset.replaceOnType = 'false';
        });

        cell.addEventListener('focus', function() {
            isEditingCell = true;
            lastEditAt = Date.now();
            const itemKey = this.getAttribute('data-item-key');
            const field = this.getAttribute('data-field');
            if (itemKey && field) {
                pendingEdits.set(`${itemKey}::${field}`, this.textContent.trim());
            }
            // Spreadsheet-style: select all text on focus when replace-on-type
            if (this.dataset.replaceOnType === 'false') return;
            requestAnimationFrame(() => {
                const range = document.createRange();
                range.selectNodeContents(this);
                const selection = window.getSelection();
                selection.removeAllRanges();
                selection.addRange(range);
            });
        });

        cell.addEventListener('blur', async function(e) {
            isEditingCell = false;
            lastEditAt = Date.now();
            // Skip if we're in the middle of a batch paste operation
            if (isBatchPasting) {
                e.stopImmediatePropagation();
                return;
            }

            const itemName = this.getAttribute('data-item-name');
            const itemKey = this.getAttribute('data-item-key');
            const rawValue = this.textContent.trim();
            const value = stripClickModeToken(rawValue);
            if (value !== rawValue) {
                this.textContent = value;
            }
            // Keep pending edits until save succeeds

            if (field === 'sold_price') {
                // Clean the value: remove $ and other non-numeric characters except decimal point
                let cleanedValue = value.replace(/[^0-9.]/g, '');
                let soldPrice = null;
                if (cleanedValue) {
                    soldPrice = parseFloat(cleanedValue);
                    if (isNaN(soldPrice)) {
                        // If invalid, revert to original value and alert
                        this.textContent = originalValue;
                        alert('Invalid sold price value. Please enter a number.');
                        return;
                    }
                }

                // Save state for undo
                saveState();

                try {
                    const response = await fetch('/api/items/sold-price', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({
                            item_name: itemName,
                            sold_price: soldPrice !== null ? soldPrice.toFixed(2) : '',
                            show_id: currentShowId
                        })
                    });
                    if (response.ok) {
                        // Update the cell content directly with formatted value
                        this.textContent = soldPrice !== null ? '$' + soldPrice.toFixed(2) : '';
                        // Update items array
                        const item = items.find(i => i.item_name === itemName);
                        if (item) {
                            item.sold_price = soldPrice !== null ? '$' + soldPrice.toFixed(2) : '';
                            item.sold_price_float = soldPrice;
                        }
                        // Update totals and profit
                        updateTotals();
                        // Re-render net revenue and profit for this row
                        const row = this.closest('tr');
                        updateRowCalculations(row, item);
                    } else {
                        const error = await response.json();
                        alert('Error updating sold price: ' + (error.error || 'Unknown error'));
                        await loadData(); // Reload to restore original value on error
                    }
                } catch (error) {
                    console.error('Error updating sold price:', error);
                    alert('Error updating sold price: ' + error.message);
                    await loadData(); // Reload to restore original value on error
                }
            } else if (field === 'sold_timestamp') {
                saveState();
                try {
                    const response = await fetch('/api/items/sold-time', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({
                            item_name: itemName,
                            sold_timestamp: value,
                            show_id: currentShowId
                        })
                    });
                    if (response.ok) {
                        const item = items.find(i => i.item_name === itemName);
                        if (item) {
                            item.sold_timestamp = value;
                        }
                    } else {
                        const error = await response.json();
                        alert('Error updating sold time: ' + (error.error || 'Unknown error'));
                        await loadData();
                    }
                } catch (error) {
                    console.error('Error updating sold time:', error);
                    alert('Error updating sold time: ' + error.message);
                    await loadData();
                }
            } else if (field === 'viewers') {
                saveState();
                try {
                    const response = await fetch('/api/items/viewers', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({
                            item_name: itemName,
                            viewers: value,
                            show_id: currentShowId
                        })
                    });
                    if (response.ok) {
                        const item = items.find(i => i.item_name === itemName);
                        if (item) {
                            item.viewers = value;
                        }
                    } else {
                        const error = await response.json();
                        alert('Error updating viewers: ' + (error.error || 'Unknown error'));
                        await loadData();
                    }
                } catch (error) {
                    console.error('Error updating viewers:', error);
                    alert('Error updating viewers: ' + error.message);
                    await loadData();
                }
            } else if (field === 'cost') {
                // Clean the value: remove $ and other non-numeric characters except decimal point
                let cleanedValue = value.replace(/[^0-9.]/g, '');
                let cost = null;
                if (cleanedValue) {
                    cost = parseFloat(cleanedValue);
                    if (isNaN(cost)) {
                        // Invalid - restore original value
                        await loadData();
                        return;
                    }
                }

                // Save state for undo
                saveState();

                // Store the value we're saving to restore it after reload
                const savedValue = cost !== null ? cost.toFixed(2) : '';

                try {
                    const response = await fetch('/api/items/cost', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({
                            item_names: [itemName],
                            cost: cost,
                            show_id: currentShowId
                        })
                    });
                    if (response.ok) {
                        // Update the cell immediately with formatted value
                        this.textContent = savedValue;
                        // Update items array
                        const item = items.find(i => i.item_name === itemName);
                        if (item) {
                            item.cost = cost;
                        }
                        // Update totals and profit
                        updateTotals();
                        // Re-render net revenue and profit for this row
                        const row = this.closest('tr');
                        updateRowCalculations(row, item);
                    } else {
                        // Error - reload to restore
                        await loadData();
                    }
                } catch (error) {
                    console.error('Error updating cost:', error);
                    // Error - reload to restore
                    await loadData();
                }
            } else if (field === 'sku') {
                const index = parseInt(this.getAttribute('data-index'), 10);
                await saveSkuValue({
                    itemName,
                    itemKey,
                    value,
                    index,
                    element: this
                });
            } else if (field === 'notes') {
                // Save state for undo
                saveState();

                try {
                    const response = await fetch('/api/items/notes', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({
                            item_name: itemName,
                            notes: value || null,
                            show_id: currentShowId
                        })
                    });
                    if (response.ok) {
                        // Update local state
                        const item = items.find(i => i.item_name === itemName);
                        if (item) {
                            item.notes = value || null;
                        }
                        this.setAttribute('data-last-saved', value);
                        if (itemKey && field) {
                            pendingEdits.delete(`${itemKey}::${field}`);
                        }
                    } else {
                        alert('Error updating notes');
                    }
                } catch (error) {
                    console.error('Error updating notes:', error);
                    alert('Error updating notes: ' + error.message);
                }
            } else if (field === 'buyer') {
                // Save state for undo
                saveState();

                try {
                    const response = await fetch('/api/items/buyer', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({
                            item_name: itemName,
                            buyer: value || null,
                            show_id: currentShowId
                        })
                    });
                    if (response.ok) {
                        const item = items.find(i => i.item_name === itemName);
                        if (item) {
                            item.buyer = value || null;
                        }
                    } else {
                        await loadData();
                    }
                } catch (error) {
                    console.error('Error updating buyer:', error);
                    await loadData();
                }
            } else if (field === 'order_id') {
                // Save state for undo
                saveState();

                try {
                    const response = await fetch('/api/items/order-id', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({
                            item_name: itemName,
                            order_id: value || null,
                            show_id: currentShowId
                        })
                    });
                    if (response.ok) {
                        const item = items.find(i => i.item_name === itemName);
                        if (item) {
                            item.order_id = value || null;
                        }
                    } else {
                        await loadData();
                    }
                } catch (error) {
                    console.error('Error updating order ID:', error);
                    await loadData();
                }
            } else if (field === 'pinned_message') {
                // Save state for undo
                saveState();

                try {
                    const response = await fetch('/api/items/pinned-message', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({
                            item_name: itemName,
                            pinned_message: value,
                            show_id: currentShowId
                        })
                    });
                    if (response.ok) {
                        const item = items.find(i => i.item_name === itemName);
                        if (item) {
                            item.pinned_message = value;
                        }
                        this.setAttribute('data-last-saved', value);
                        if (itemKey && field) {
                            pendingEdits.delete(`${itemKey}::${field}`);
                        }
                    } else {
                        await loadData();
                    }
                } catch (error) {
                    console.error('Error updating pinned message:', error);
                    await loadData();
                }
            }
            // Other fields (net_revenue, profit) are visual-only for now
        });

        // Debounced autosave while typing for specific fields
        if (autosaveFields.has(field)) {
            cell.addEventListener('input', function() {
                if (isBatchPasting) return;
                lastEditAt = Date.now();
                const currentValue = this.textContent.trim();
                const lastSaved = this.getAttribute('data-last-saved') || '';
                if (currentValue === lastSaved) return;
                const itemKey = this.getAttribute('data-item-key');
                const field = this.getAttribute('data-field');
                if (itemKey && field) {
                    pendingEdits.set(`${itemKey}::${field}`, currentValue);
                }
                const itemName = this.getAttribute('data-item-name');
                const item = items.find(i => i.item_name === itemName);
                if (item && field) {
                    item[field] = currentValue;
                }

                const existingTimer = autosaveTimers.get(this);
                if (existingTimer) {
                    clearTimeout(existingTimer);
                }

                const timer = setTimeout(async () => {
                    // Trigger save without forcing blur
                    const itemName = this.getAttribute('data-item-name');
                    const field = this.getAttribute('data-field');
                    const value = this.textContent.trim();

                    if (!itemName) return;
                    if ((this.getAttribute('data-last-saved') || '') === value) return;

                    try {
                        if (field === 'sku') {
                            const response = await fetch('/api/items/sku', {
                                method: 'POST',
                                headers: {'Content-Type': 'application/json'},
                                body: JSON.stringify({
                                    item_name: itemName,
                                    sku: value || null,
                                    show_id: currentShowId
                                })
                            });
                            if (response.ok) {
                                const item = items.find(i => i.item_name === itemName);
                                if (item) item.sku = value || null;
                                this.setAttribute('data-last-saved', value);
                            }
                        } else if (field === 'notes') {
                            const response = await fetch('/api/items/notes', {
                                method: 'POST',
                                headers: {'Content-Type': 'application/json'},
                                body: JSON.stringify({
                                    item_name: itemName,
                                    notes: value || null,
                                    show_id: currentShowId
                                })
                            });
                            if (response.ok) {
                                const item = items.find(i => i.item_name === itemName);
                                if (item) item.notes = value || null;
                                this.setAttribute('data-last-saved', value);
                            }
                        } else if (field === 'pinned_message') {
                            const response = await fetch('/api/items/pinned-message', {
                                method: 'POST',
                                headers: {'Content-Type': 'application/json'},
                                body: JSON.stringify({
                                    item_name: itemName,
                                    pinned_message: value,
                                    show_id: currentShowId
                                })
                            });
                            if (response.ok) {
                                const item = items.find(i => i.item_name === itemName);
                                if (item) item.pinned_message = value;
                                this.setAttribute('data-last-saved', value);
                            }
                        }
                    } catch (error) {
                        console.error('Error autosaving cell:', error);
                    }
                }, 500);

                autosaveTimers.set(this, timer);
            });
        }

        // Handle Enter key and ensure undo works
        cell.addEventListener('keydown', function(e) {
            // Allow undo (Cmd+Z / Ctrl+Z) - don't prevent default
            if ((e.metaKey || e.ctrlKey) && e.key === 'z') {
                // Let browser handle undo naturally
                return;
            }
            if (
                this.dataset.replaceOnType === 'true' &&
                !e.metaKey &&
                !e.ctrlKey &&
                !e.altKey &&
                e.key.length === 1
            ) {
                e.preventDefault();
                this.textContent = e.key;
                this.dataset.replaceOnType = 'false';
                const range = document.createRange();
                range.selectNodeContents(this);
                range.collapse(false);
                const selection = window.getSelection();
                selection.removeAllRanges();
                selection.addRange(range);
                return;
            }
            if (this.dataset.replaceOnType === 'true' && (e.key === 'Backspace' || e.key === 'Delete')) {
                e.preventDefault();
                this.textContent = '';
                this.dataset.replaceOnType = 'false';
                return;
            }
            if (e.key === 'Enter') {
                e.preventDefault();
                this.blur();
            }
        });

        cell.addEventListener('paste', function(e) {
            if (this.dataset.replaceOnType !== 'true') return;
            e.preventDefault();
            const pastedText = (e.clipboardData || window.clipboardData).getData('text');
            this.textContent = pastedText;
            this.dataset.replaceOnType = 'false';
            const range = document.createRange();
            range.selectNodeContents(this);
            range.collapse(false);
            const selection = window.getSelection();
            selection.removeAllRanges();
            selection.addRange(range);
        });
    });
}

function attachCancelledStatusHandlers() {
    // Handle cancelled/failed status dropdowns
    document.querySelectorAll('.cancelled-status-select').forEach(select => {
        select.addEventListener('change', async function() {
            const itemName = this.getAttribute('data-item-name');
            const rawValue = this.value;
            const statusValue = rawValue === STATUS_PROCESSED ? '' : rawValue;

            if (!itemName) return;

            try {
                const response = await fetch('/api/items/cancelled-status', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        item_name: itemName,
                        cancelled_status: statusValue || null,
                        show_id: currentShowId
                    })
                });
                if (response.ok) {
                    const item = items.find(i => i.item_name === itemName);
                    if (item) {
                        item.cancelled_status = statusValue || null;
                    }
                } else {
                    await loadData();
                }
            } catch (error) {
                console.error('Error updating cancelled status:', error);
                await loadData();
            }
        });
    });
}

function attachPresetTextHandlers() {
    // Handle preset text divs
    document.querySelectorAll('.preset-text').forEach(display => {
        display.addEventListener('paste', async function(e) {
            e.preventDefault();
            const pastedText = (e.clipboardData || window.clipboardData).getData('text').trim();
            if (!pastedText) return;

            const index = parseInt(this.getAttribute('data-index'));
            if (isNaN(index)) return;

            // Find matching preset
            const matchingPreset = presets.find(p => p.name === pastedText);
            if (matchingPreset) {
                await applyPresetToRow(index, matchingPreset.name, matchingPreset.cost);
            } else {
                // If no match, just update the display
                this.textContent = pastedText;
            }
        });

        display.addEventListener('blur', async function() {
            const pastedText = this.textContent.trim();
            const index = parseInt(this.getAttribute('data-index'));
            const itemName = this.getAttribute('data-item-name');

            if (!pastedText || isNaN(index) || !itemName) {
                // If cleared, save empty preset name
                if (!pastedText) {
                    try {
                        await fetch('/api/items/cost', {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({
                                item_names: [itemName],
                                preset_name: '',
                                show_id: currentShowId
                            })
                        });
                        // Update items array
                        const item = items.find(i => i.item_name === itemName);
                        if (item) {
                            item.preset_name = '';
                        }
                    } catch (error) {
                        console.error('Error clearing preset:', error);
                    }
                }
                return;
            }

            // Find matching preset
            const matchingPreset = presets.find(p => p.name === pastedText);
            if (matchingPreset) {
                await applyPresetToRow(index, matchingPreset.name, matchingPreset.cost);
            } else {
                // Save the preset name even if no matching preset found
                try {
                    await fetch('/api/items/cost', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({
                            item_names: [itemName],
                            preset_name: pastedText,
                            show_id: currentShowId
                        })
                    });
                    // Update items array
                    const item = items.find(i => i.item_name === itemName);
                    if (item) {
                        item.preset_name = pastedText;
                        maybeAddPresetSticky(item.item_name);
                    }
                } catch (error) {
                    console.error('Error saving preset name:', error);
                }
            }
        });
    });
}

function attachSkuSelectHandlers() {
    document.querySelectorAll('.sku-select').forEach(select => {
        select.addEventListener('mousedown', function(e) {
            e.stopPropagation();
        });
        select.addEventListener('click', function(e) {
            e.stopPropagation();
        });
        select.addEventListener('change', async function() {
            const cell = this.closest('.sku-cell');
            const textEl = cell ? cell.querySelector('.sku-text') : null;
            const itemName = this.getAttribute('data-item-name');
            const itemKey = this.getAttribute('data-item-key');
            const index = parseInt(this.getAttribute('data-index'), 10);
            const rawValue = (this.value || '').trim();
            const value = stripClickModeToken(rawValue);
            if (textEl) {
                textEl.textContent = value;
            }
            await saveSkuValue({ itemName, itemKey, value, index, element: textEl });
        });
    });
}

function attachSkuTextInputHandlers() {
    document.querySelectorAll('.sku-text').forEach(skuText => {
        skuText.onmousedown = null;
        skuText.onclick = null;
        skuText.addEventListener('input', function() {
            const cell = skuText.closest('.sku-cell');
            const itemName = skuText.getAttribute('data-item-name');
            const itemKey = skuText.getAttribute('data-item-key');
            const index = parseInt(skuText.getAttribute('data-index'), 10);
            const rawValue = (skuText.textContent || '').trim();
            const value = stripClickModeToken(rawValue);
            if (!cell || !itemName) return;
            if (!skuText.dataset.saveTimer) {
                skuText.dataset.saveTimer = '0';
            }
            if (skuText.dataset.saveTimer) {
                clearTimeout(Number(skuText.dataset.saveTimer));
            }
            const timer = setTimeout(async () => {
                await saveSkuValue({ itemName, itemKey, value, index, element: skuText });
            }, 700);
            skuText.dataset.saveTimer = String(timer);
        });
    });
}

function attachEditableListeners() {
    attachContenteditableCellListeners();
    attachCancelledStatusHandlers();
    attachPresetTextHandlers();
    attachSkuSelectHandlers();
    attachSkuTextInputHandlers();
}

function renderPresets() {
    const presetsList = document.getElementById('presets-list');
    const presetSelect = document.getElementById('preset-select');
    
    presetsList.innerHTML = presets.map(preset => `
        <span class="preset-item">
            ${escapeHtml(preset.name)} = $${preset.cost.toFixed(2)}
            <button onclick="deletePreset('${escapeHtml(preset.name)}')">×</button>
        </span>
    `).join('');
    
    presetSelect.innerHTML = '<option value="">Select preset...</option>' +
        presets.map(preset => 
            `<option value="${escapeHtml(preset.name)}|${preset.cost}">${escapeHtml(preset.name)} ($${preset.cost.toFixed(2)})</option>`
        ).join('');
}

async function addPreset() {
    const name = document.getElementById('preset-name').value.trim();
    const cost = parseFloat(document.getElementById('preset-cost').value);
    
    if (!name || isNaN(cost)) {
        alert('Please enter both name and cost');
        return;
    }
    
    await savePreset(name, cost);
    
    document.getElementById('preset-name').value = '';
    document.getElementById('preset-cost').value = '';
}

async function addPresetQuick() {
    const name = document.getElementById('quick-preset-name').value.trim();
    const cost = parseFloat(document.getElementById('quick-preset-cost').value);
    
    if (!name || isNaN(cost)) {
        alert('Please enter both preset name and cost');
        return;
    }
    
    const saved = await savePreset(name, cost, true);
    if (!saved) return;
    
    document.getElementById('quick-preset-name').value = '';
    document.getElementById('quick-preset-cost').value = '';
}

async function savePreset(name, cost, shouldReload = true) {
    try {
        const res = await fetch('/api/presets', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({name, cost})
        });
        
        if (res.ok) {
            if (shouldReload) {
            loadData();
            }
            return true;
        } else {
            const error = await res.json();
            alert(error.error || 'Error adding preset');
            return false;
        }
    } catch (error) {
        alert('Error adding preset: ' + error.message);
        return false;
    }
}

async function savePresetFromRow(index) {
    const row = document.querySelector(`.item-preset-name[data-index="${index}"]`).closest('tr');
    const nameInput = row.querySelector('.item-preset-name');
    const costInput = row.querySelector('.item-preset-cost');
    
    const name = nameInput.value.trim();
    const cost = parseFloat(costInput.value);
    
    if (!name || isNaN(cost)) {
        alert('Please enter both preset name and cost');
        return;
    }
    
    const saved = await savePreset(name, cost, false);
    if (!saved) return;
    
    const existingPreset = presets.find(preset => preset.name === name);
    if (existingPreset) {
        existingPreset.cost = cost;
    } else {
        presets.push({ name, cost });
    }
    renderPresets();
    
    const select = row.querySelector('.item-preset-select');
    if (select) {
        const optionValue = `${name}|${cost}`;
        const hasOption = Array.from(select.options).some(option => option.value === optionValue);
        if (!hasOption) {
            const option = document.createElement('option');
            option.value = optionValue;
            option.textContent = `${name} ($${cost.toFixed(2)})`;
            select.appendChild(option);
        }
    }
    
    await applyPresetToRow(index, name, cost);
    
    // Don't clear the inputs - keep the values so user can see what was saved
}

async function applyPresetToRow(index, presetName, presetCost) {
        const row = document.querySelector(`.item-preset-name[data-index="${index}"]`).closest('tr');
    if (!row) return;
    
        const nameInput = row.querySelector('.item-preset-name');
        const costInput = row.querySelector('.item-preset-cost');
    const presetDisplay = row.querySelector('.preset-text');
    const select = row.querySelector('.item-preset-select');
    const costCell = row.querySelector('.editable-cost');
    const itemName = costCell ? costCell.getAttribute('data-item-name') : null;
        
    // Update the preset inputs
        if (nameInput) {
            nameInput.value = presetName;
        }
        if (costInput) {
            costInput.value = presetCost.toFixed(2);
        }
        
    // Update the preset display
    if (presetDisplay) {
        presetDisplay.textContent = presetName;
    }
    
    // Update the dropdown to show the selected preset
        if (select) {
        const optionValue = `${presetName}|${presetCost}`;
        select.value = optionValue;
    }
    
    // Update the actual Cost cell and save to database
    if (costCell && itemName) {
        maybeAddPresetSticky(itemName);
        // Save state for undo
        saveState();
        
        // Update the cost cell visually
        costCell.textContent = presetCost.toFixed(2);
        
        // Save to database (cost AND preset_name)
        try {
            const response = await fetch('/api/items/cost', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    item_names: [itemName],
                    cost: presetCost,
                    preset_name: presetName,
                    show_id: currentShowId
                })
            });
            
            if (response.ok) {
                // Update the items array
                const item = items.find(i => i.item_name === itemName);
                if (item) {
                    item.cost = presetCost;
                    item.preset_name = presetName;
                    maybeAddPresetSticky(item.item_name);
                learnAiFromItem(item.item_name);
                    // Update calculations for this row only
                    updateRowCalculations(row, item);
                }
                // Recalculate totals without full rerender (prevents image refresh)
                updateTotals();
            }
        } catch (error) {
            console.error('Error updating cost:', error);
        }
    }
}

async function handlePresetSelect(index, value) {
    if (!value) return;
    
    const parts = value.split('|');
    if (parts.length === 2) {
        const presetName = parts[0];
        const presetCost = parseFloat(parts[1]);
        await applyPresetToRow(index, presetName, presetCost);
    }
}

async function deletePreset(name) {
    if (!confirm(`Delete preset "${name}"?`)) return;
    
    try {
        await fetch(`/api/presets/${encodeURIComponent(name)}`, {
            method: 'DELETE'
        });
        loadData();
    } catch (error) {
        alert('Error deleting preset: ' + error.message);
    }
}

async function applyPresetToSelected() {
    const selected = getSelectedItems();
    if (selected.length === 0) {
        alert('Please select at least one item');
        return;
    }
    
    const presetValue = document.getElementById('preset-select').value;
    if (!presetValue) {
        alert('Please select a preset');
        return;
    }
    
    const parts = presetValue.split('|');
    if (parts.length !== 2) {
        alert('Invalid preset value');
        return;
    }
    
    const presetName = parts[0];
    const cost = parseFloat(parts[1]);
    if (isNaN(cost)) {
        alert('Invalid preset cost');
        return;
    }
    
    const itemNames = selected.map(index => displayItems[index].item_name);
    
    // Save state for undo
    saveState();
    
    try {
        const res = await fetch('/api/items/cost', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                item_names: itemNames,
                cost,
                preset_name: presetName,
                show_id: currentShowId
            })
        });
        
        if (res.ok) {
            document.getElementById('preset-select').value = '';
            itemNames.forEach(name => {
                const item = items.find(i => i.item_name === name);
                if (item) {
                    item.preset_name = presetName;
                    item.cost = cost;
                    learnAiFromItem(name);
                }
            });
            loadData();
        } else {
            alert('Error applying preset');
        }
    } catch (error) {
        alert('Error applying preset: ' + error.message);
    }
}

let selectedRowIndices = new Set();
let lastSelectedRowIndex = null;
let lastActiveRowIndex = null;
let isBatchPasting = false;
let lastRowNumberClickAt = 0;

function getRowIndexFromCell(cell) {
    const row = cell ? cell.closest('tr') : null;
    const tbody = document.getElementById('items-table-body');
    if (!row || !tbody) return null;
    const dataRows = Array.from(tbody.querySelectorAll('tr')).filter(
        (tr) => !tr.classList.contains('totals-row')
    );
    const index = dataRows.indexOf(row);
    return index >= 0 ? index : null;
}

function getSelectedItems() {
    if (selectedRowIndices.size > 0) {
        return Array.from(selectedRowIndices);
    }
    if (typeof selectedCells !== 'undefined' && selectedCells.length > 0) {
        const indices = new Set();
        selectedCells.forEach((cell) => {
            const index = getRowIndexFromCell(cell);
            if (index !== null) indices.add(index);
        });
        return Array.from(indices);
    }
    if (lastActiveRowIndex !== null && lastActiveRowIndex !== undefined) {
        return [lastActiveRowIndex];
    }
    return [];
}

function selectRow(index, addToSelection = false) {
    if (!addToSelection) {
        // If clicking the same row that's already selected, toggle it off
        if (selectedRowIndices.has(index) && selectedRowIndices.size === 1) {
            selectedRowIndices.clear();
            lastSelectedRowIndex = null;
            updateRowSelection();
            return;
        }
        selectedRowIndices.clear();
    }
    clearCellSelections();
    selectedRowIndices.add(index);
    lastSelectedRowIndex = index;
    updateRowSelection();
}

function selectRowRange(startIndex, endIndex) {
    const start = Math.min(startIndex, endIndex);
    const end = Math.max(startIndex, endIndex);
    clearCellSelections();
    for (let i = start; i <= end; i++) {
        selectedRowIndices.add(i);
    }
    lastSelectedRowIndex = endIndex;
    updateRowSelection();
}

function toggleRow(index) {
    clearCellSelections();
    if (selectedRowIndices.has(index)) {
        selectedRowIndices.delete(index);
    } else {
        selectedRowIndices.add(index);
    }
    lastSelectedRowIndex = index;
    updateRowSelection();
}

function updateRowSelection() {
    // Update visual selection
    document.querySelectorAll('.row-number').forEach((rowNum, idx) => {
        const row = rowNum.closest('tr');
        if (selectedRowIndices.has(idx)) {
            rowNum.style.backgroundColor = 'var(--selected-bg, #007bff)';
            rowNum.style.color = 'white';
            if (row) row.classList.add('row-selected');
        } else {
            rowNum.style.backgroundColor = '';
            rowNum.style.color = '';
            if (row) row.classList.remove('row-selected');
        }
    });
    
    const count = selectedRowIndices.size;
    document.getElementById('selected-count').textContent = `${count} selected`;
}

function updateSelectedCount() {
    updateRowSelection();
}

let deleteConfirmResolver = null;

function confirmDeleteRows(count) {
    const skip = localStorage.getItem('skipRowDeleteConfirm') === 'true';
    if (skip) {
        return Promise.resolve(true);
    }

    const overlay = document.getElementById('delete-confirm-overlay');
    const message = document.getElementById('delete-confirm-message');
    const skipCheckbox = document.getElementById('delete-confirm-skip');
    const okBtn = document.getElementById('delete-confirm-ok');
    const cancelBtn = document.getElementById('delete-confirm-cancel');

    if (!overlay || !message || !okBtn || !cancelBtn || !skipCheckbox) {
        return Promise.resolve(window.confirm(`Delete ${count} item(s)?`));
    }

    message.textContent = `Delete ${count} item(s)? This cannot be undone.`;
    skipCheckbox.checked = false;
    overlay.style.display = 'flex';

    return new Promise((resolve) => {
        deleteConfirmResolver = resolve;
    });
}

function closeDeleteConfirm(confirmed) {
    const overlay = document.getElementById('delete-confirm-overlay');
    const skipCheckbox = document.getElementById('delete-confirm-skip');
    if (overlay) overlay.style.display = 'none';

    if (confirmed && skipCheckbox && skipCheckbox.checked) {
        localStorage.setItem('skipRowDeleteConfirm', 'true');
    }

    if (deleteConfirmResolver) {
        deleteConfirmResolver(confirmed);
        deleteConfirmResolver = null;
    }
}

function initDeleteConfirmModal() {
    const okBtn = document.getElementById('delete-confirm-ok');
    const cancelBtn = document.getElementById('delete-confirm-cancel');
    const overlay = document.getElementById('delete-confirm-overlay');

    if (okBtn) {
        okBtn.addEventListener('click', () => closeDeleteConfirm(true));
    }
    if (cancelBtn) {
        cancelBtn.addEventListener('click', () => closeDeleteConfirm(false));
    }
    if (overlay) {
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) {
                closeDeleteConfirm(false);
            }
        });
    }
}

function clearCellSelections() {
    document.querySelectorAll('.selected, .selecting, .cell-selection-start').forEach(el => {
        el.classList.remove('selected', 'selecting', 'cell-selection-start');
    });
    selectedCells = [];
}

async function deleteSelected() {
    const selected = getSelectedItems();
    if (selected.length === 0) {
        alert('Please select at least one item to delete');
        return;
    }
    
    const confirmed = await confirmDeleteRows(selected.length);
    if (!confirmed) return;
    
    const itemNames = selected
        .map(index => displayItems[index])
        .filter(Boolean)
        .map(item => item.item_name);
    if (itemNames.length === 0) {
        alert('Please select a valid row to delete');
        return;
    }
    
    const deletedRows = selected
        .map(index => displayItems[index])
        .filter(Boolean)
        .map(item => ({
            timestamp: item.timestamp || new Date().toISOString(),
            item_title: item.item_name,
            pinned_text: item.pinned_message || '',
            filename: item.filename || '__no_image__',
            sold_price: item.sold_price || '',
            sold_timestamp: item.sold_timestamp || '',
            viewers: item.viewers || ''
        }));
    
    // Save state for undo
    saveState({ deletedRows });
    
    try {
        const res = await fetch('/api/items/delete', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                item_names: itemNames,
                show_id: currentShowId
            })
        });
        
        if (res.ok) {
            const namesSet = new Set(itemNames);
            displayItems = displayItems.filter(item => !namesSet.has(item.item_name));
            items = items.filter(item => !namesSet.has(item.item_name));
            selectedRowIndices.clear();
            lastSelectedRowIndex = null;
            renderTable();
            updateTotals();
            updateRowSelection();
        } else {
            const error = await res.json();
            alert('Error deleting items: ' + (error.error || 'Unknown error'));
        }
    } catch (error) {
        alert('Error deleting items: ' + error.message);
    }
}

async function addManualRow() {
    if (!currentShowId) {
        alert('Please select a show first.');
        return;
    }
    const name = prompt('Enter item name for new row:');
    if (!name || !name.trim()) return;
    try {
        const res = await fetch('/api/items/add', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                show_id: currentShowId,
                item_name: name.trim()
            })
        });
        const data = await res.json();
        if (!res.ok) {
            alert(data.error || 'Failed to add row');
            return;
        }
        const item = data.item;
        if (item) {
            items.push(item);
            renderTable();
            updateTotals();
            const index = displayItems.findIndex(i => i.item_name === item.item_name);
            if (index >= 0) {
                selectedRowIndices.clear();
                selectedRowIndices.add(index);
                lastSelectedRowIndex = index;
                updateRowSelection();
            }
        } else {
            await loadData();
        }
    } catch (error) {
        alert('Error adding row: ' + error.message);
    }
}

async function saveImageForItem(itemName, imageRef, cell) {
    if (!itemName || !imageRef) return;
    try {
        const res = await fetch('/api/items/image', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                show_id: currentShowId,
                item_name: itemName,
                image_ref: imageRef
            })
        });
        const data = await res.json();
        if (!res.ok) {
            alert(data.error || 'Failed to update image');
            return;
        }
        const item = items.find(i => i.item_name === itemName);
        if (item) {
            item.image = imageRef;
            item.filename = imageRef;
        }
        if (cell) {
            cell.innerHTML = `<img src="${imageRef}" class="image-thumb" onclick="handleImageClick(event, '${imageRef}')" alt="Item image">`;
        } else {
            renderTable();
        }
    } catch (error) {
        alert('Error updating image: ' + error.message);
    }
}

function showImage(imagePath) {
    document.getElementById('modal-image').src = imagePath;
    document.getElementById('image-modal').style.display = 'block';
}

function closeImageModal() {
    document.getElementById('image-modal').style.display = 'none';
}

function formatSoldTimestamp(value) {
    if (!value) return '';
    try {
        const date = new Date(value);
        if (isNaN(date.getTime())) return value;
        const now = new Date();
        const isToday = date.toDateString() === now.toDateString();
        if (isToday) {
            return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        }
        return date.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
    } catch (e) {
        return value;
    }
}

function getVisibleColumnDefs() {
    return getVisibleColumns().map(colId => {
        const column = columnConfig.find(col => col.id === colId);
        return column ? { id: colId, label: column.label } : { id: colId, label: colId };
    });
}

function getPrintableColumns(mode) {
    const visible = getVisibleColumnDefs();
    if (mode === 'employee') {
        const allowed = new Set(['item_name', 'order_id', 'buyer', 'sku', 'notes', 'pinned_message', 'image']);
        return visible.filter(col => allowed.has(col.id));
    }
    return visible;
}

function getPrintableRows(scope) {
    if (scope === 'selection') {
        return displayItems && displayItems.length ? displayItems : getFilteredItems(items);
    }
    const allItems = items.slice();
    allItems.sort((a, b) => {
        const aTime = a.sold_timestamp ? new Date(a.sold_timestamp).getTime() : 0;
        const bTime = b.sold_timestamp ? new Date(b.sold_timestamp).getTime() : 0;
        return aTime - bTime;
    });
    return allItems;
}

function getCurrentShowTitle() {
    const showSelect = document.getElementById('show-select');
    if (!showSelect || showSelect.selectedIndex < 0) return 'Show';
    const text = showSelect.options[showSelect.selectedIndex]?.text || 'Show';
    return text;
}

function buildPrintableTable(columns, rows) {
    const header = columns.map(col => `<th>${escapeHtml(col.label)}</th>`).join('');
    const body = rows.map(item => {
        const cells = columns.map(col => {
            if (col.id === 'sold_timestamp') {
                return `<td>${escapeHtml(formatSoldTimestamp(item.sold_timestamp))}</td>`;
            }
            if (col.id === 'image') {
                // Use /thumbnails/ for print — compressed JPEG, much smaller PDF
                const thumbUrl = item.image ? item.image.replace('/screenshots/', '/thumbnails/') : '';
                return thumbUrl
                    ? `<td><img src="${thumbUrl}" style="width:120px;height:120px;object-fit:cover;border-radius:6px;"></td>`
                    : `<td></td>`;
            }
            const value = item[col.id] || '';
            return `<td>${escapeHtml(String(value))}</td>`;
        }).join('');
        return `<tr>${cells}</tr>`;
    }).join('');

    return `
        <table>
            <thead><tr>${header}</tr></thead>
            <tbody>${body}</tbody>
        </table>
    `;
}

function printTablePdf(mode, scope) {
    const columns = getPrintableColumns(mode);
    const rows = getPrintableRows(scope);
    const showTitle = getCurrentShowTitle();
    const scopeLabel = scope === 'selection' ? 'Selection' : 'All (Sorted by Sold Time)';

    const html = `
        <html>
        <head>
            <title>${escapeHtml(showTitle)}</title>
            <style>
                body { font-family: Arial, sans-serif; color: #111; margin: 24px; }
                h1 { font-size: 18px; margin: 0 0 6px; }
                h2 { font-size: 12px; color: #555; margin: 0 0 16px; font-weight: normal; }
                table { width: 100%; border-collapse: collapse; }
                th, td { border: 1px solid #ccc; padding: 6px 8px; font-size: 11px; vertical-align: top; }
                th { background: #f2f2f2; text-align: left; }
                img { display: block; }
            </style>
        </head>
        <body>
            <h1>${escapeHtml(showTitle)}</h1>
            <h2>${escapeHtml(scopeLabel)}</h2>
            ${buildPrintableTable(columns, rows)}
        </body>
        </html>
    `;

    const printWindow = window.open('', '_blank', 'width=1200,height=800');
    if (!printWindow) {
        alert('Pop-up blocked. Please allow pop-ups to print.');
        return;
    }
    printWindow.document.open();
    printWindow.document.write(html);
    printWindow.document.close();
    printWindow.focus();
    // Use setTimeout to ensure content is rendered before printing
    setTimeout(function() {
        printWindow.print();
    }, 500);
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function updateItemField(itemName, field, value) {
    if (!itemName || !field) return;
    const item = items.find(i => i.item_name === itemName);
    if (item) {
        item[field] = value;
    }
}

function getItemKey(name) {
    return encodeURIComponent(name || '');
}

// Theme toggle
function toggleTheme() {
    const currentTheme = document.documentElement.getAttribute('data-theme');
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', newTheme);
    document.body.setAttribute('data-theme', newTheme);
    document.documentElement.classList.toggle('dark', newTheme === 'dark');
    localStorage.setItem('theme', newTheme);
    updateThemeButton();
}

function updateThemeButton() {
    const themeButton = document.querySelector('[data-role="theme-toggle"]');
    const currentTheme = document.body.getAttribute('data-theme');
    if (themeButton) {
        themeButton.textContent = currentTheme === 'dark' ? '☀️' : '🌙';
    }
}

// Click Mode (single-click paste)
let clickModeActive = false;
let clickModeValue = '';
let isEditingCell = false;
let lastEditAt = 0;
const pendingEdits = new Map();
const PENDING_EDITS_KEY = 'pendingEdits';
let pendingEditsSaveTimer = null;

function savePendingEdits() {
    try {
        const payload = Array.from(pendingEdits.entries());
        localStorage.setItem(PENDING_EDITS_KEY, JSON.stringify(payload));
    } catch (e) {
        console.error('Failed to save pending edits:', e);
    }
}

function schedulePendingEditsSave() {
    if (pendingEditsSaveTimer) {
        clearTimeout(pendingEditsSaveTimer);
    }
    pendingEditsSaveTimer = setTimeout(() => {
        savePendingEdits();
    }, 250);
}

function loadPendingEdits() {
    const saved = localStorage.getItem(PENDING_EDITS_KEY);
    if (!saved) return;
    try {
        const entries = JSON.parse(saved);
        if (Array.isArray(entries)) {
            entries.forEach(([key, value]) => {
                if (typeof key === 'string') {
                    pendingEdits.set(key, value);
                }
            });
        }
    } catch (e) {
        console.error('Failed to load pending edits:', e);
    }
}

const originalPendingSet = pendingEdits.set.bind(pendingEdits);
pendingEdits.set = function(key, value) {
    const result = originalPendingSet(key, value);
    schedulePendingEditsSave();
    return result;
};
const originalPendingDelete = pendingEdits.delete.bind(pendingEdits);
pendingEdits.delete = function(key) {
    const result = originalPendingDelete(key);
    schedulePendingEditsSave();
    return result;
};

function applyPendingEditsToItems() {
    if (pendingEdits.size === 0) return;
    pendingEdits.forEach((value, key) => {
        const [itemKey, field] = key.split('::');
        const itemName = decodeURIComponent(itemKey || '');
        const item = items.find(i => i.item_name === itemName);
        if (item && field) {
            item[field] = value;
        }
    });
}

function updateClickModeButtons() {
    const text = clickModeActive ? 'Click Mode: On' : 'Click Mode: Off';
    const toggle = document.getElementById('click-mode-toggle');
    const presetToggle = document.getElementById('click-mode-preset-toggle');
    if (toggle) toggle.textContent = text;
    if (presetToggle) presetToggle.textContent = text;
}

function setClickMode(active, value = '') {
    clickModeActive = active;
    clickModeValue = active ? value : '';
    document.body.classList.toggle('click-mode-active', active);
    updateClickModeButtons();
}

function toggleClickModeFromInput() {
    if (clickModeActive) {
        setClickMode(false);
        return;
    }
    const input = document.getElementById('click-mode-input');
    const value = input ? input.value.trim() : '';
    if (!value) {
        alert('Type a click value first');
        return;
    }
    setClickMode(true, value);
}

function toggleClickModeFromPreset() {
    if (clickModeActive) {
        setClickMode(false);
        return;
    }
    const presetSelect = document.getElementById('preset-select');
    let value = '';
    if (presetSelect && presetSelect.value) {
        value = presetSelect.value.split('|')[0] || '';
    }
    if (!value) {
        const input = document.getElementById('click-mode-input');
        value = input ? input.value.trim() : '';
    }
    if (!value) {
        alert('Select a preset or type a click value');
        return;
    }
    setClickMode(true, value);
}

function addClickModeIcons() {
    const rows = document.querySelectorAll('#items-table-body tr');
    rows.forEach(row => {
        if (row.classList.contains('totals-row')) return;
        const cells = row.querySelectorAll('td');
        cells.forEach(cell => {
            if (cell.classList.contains('row-number')) return;
            if (cell.querySelector('img')) return;
            if (cell.querySelector('.cm-icon')) return;
            if (!isClickModeAllowed(cell)) return;
            cell.classList.add('cm-cell');
            const button = document.createElement('button');
            button.type = 'button';
            button.className = 'cm-icon';
            button.textContent = 'CM';
            button.setAttribute('contenteditable', 'false');
            button.title = 'Click Mode';
            cell.appendChild(button);
        });
    });
}

function scrubClickModeText() {
    document.querySelectorAll('.cm-cell').forEach(cell => {
        const nodes = Array.from(cell.childNodes);
        nodes.forEach(node => {
            if (node.nodeType === Node.TEXT_NODE && node.textContent.includes('CM')) {
                node.textContent = node.textContent.replace(/\bCM\b/g, '').replace(/\s{2,}/g, ' ').trim();
            }
        });
    });
}

function getCellValueForClickMode(cell) {
    if (!cell) return '';
    const presetDisplay = cell.querySelector('.preset-text');
    if (presetDisplay) {
        return presetDisplay.textContent.trim();
    }
    const skuText = cell.querySelector('.sku-text');
    if (skuText) {
        return skuText.textContent.trim();
    }
    const select = cell.querySelector('.cancelled-status-select');
    if (select) {
        return select.value || '';
    }
    const textNodes = Array.from(cell.childNodes).filter(node => node.nodeType === Node.TEXT_NODE);
    const text = textNodes.map(node => node.textContent).join('').trim();
    if (text) return text;
    return cell.textContent.trim();
}

function isClickModeAllowed(cell) {
    if (!cell) return false;
    const field = cell.getAttribute('data-field');
    if (field && ['sold_price', 'net_revenue', 'order_id', 'buyer'].includes(field)) {
        return false;
    }
    if (cell.querySelector('.cancelled-status-select')) {
        return true;
    }
    if (cell.contentEditable === 'true') {
        return !['sold_price', 'net_revenue', 'order_id', 'buyer'].includes(field || '');
    }
    if (cell.querySelector('.preset-text')) {
        return true;
    }
    if (cell.querySelector('.sku-text')) {
        return true;
    }
    return false;
}

function setEditableCellValue(cell, value) {
    const icon = cell.querySelector('.cm-icon');
    const textNodes = Array.from(cell.childNodes).filter(node => node.nodeType === Node.TEXT_NODE);
    let textNode = textNodes[0];
    if (!textNode) {
        textNode = document.createTextNode('');
        if (icon) {
            cell.insertBefore(textNode, icon);
        } else {
            cell.appendChild(textNode);
        }
    }
    textNode.textContent = value;
}

function applyClickModeToCell(cell, value) {
    if (!cell) return false;

    const presetText = cell.querySelector('.preset-text');
    if (presetText) {
        presetText.textContent = value;
        presetText.dispatchEvent(new Event('blur', { bubbles: true }));
        return true;
    }

    const cancelledSelect = cell.querySelector('.cancelled-status-select');
    if (cancelledSelect) {
        cancelledSelect.value = value;
        cancelledSelect.dispatchEvent(new Event('change', { bubbles: true }));
        return true;
    }

    const skuText = cell.querySelector('.sku-text');
    if (skuText) {
        skuText.textContent = value;
        const itemName = skuText.getAttribute('data-item-name');
        const itemKey = skuText.getAttribute('data-item-key');
        const index = parseInt(skuText.getAttribute('data-index'), 10);
        if (window.saveSkuValue) {
            window.saveSkuValue({
                itemName,
                itemKey,
                value,
                index,
                element: skuText
            });
        } else {
            skuText.dispatchEvent(new Event('blur', { bubbles: true }));
        }
        return true;
    }

    if (cell.contentEditable === 'true' && isClickModeAllowed(cell)) {
        setEditableCellValue(cell, value);
        cell.dispatchEvent(new Event('blur', { bubbles: true }));
        return true;
    }

    return false;
}

function applyClickModeToRow(row, value) {
    if (!row) return false;

    const selectedInRow = (selectedCells || []).filter(cell => cell && cell.closest('tr') === row);
    if (selectedInRow.length > 0) {
        let applied = false;
        selectedInRow.forEach(cell => {
            if (applyClickModeToCell(cell, value)) {
                applied = true;
            }
        });
        return applied;
    }

    const presetCell = row.querySelector('.editable-preset-cell');
    if (presetCell && applyClickModeToCell(presetCell, value)) {
        return true;
    }

    const editableCells = Array.from(row.querySelectorAll('td[contenteditable="true"]'));
    for (const cell of editableCells) {
        if (applyClickModeToCell(cell, value)) {
            return true;
        }
    }

    return false;
}

function applyClickModeToImageRow(row, value) {
    if (!row) return false;
    const presetCell = row.querySelector('.editable-preset-cell');
    if (presetCell && applyClickModeToCell(presetCell, value)) {
        return true;
    }
    const editableCells = Array.from(row.querySelectorAll('td[contenteditable="true"]'));
    for (const cell of editableCells) {
        if (applyClickModeToCell(cell, value)) {
            return true;
        }
    }
    return false;
}

function handleImageClick(event, imagePath) {
    if (clickModeActive) {
        event.preventDefault();
        event.stopPropagation();
        const row = event.target.closest('tr');
        applyClickModeToImageRow(row, clickModeValue);
        return;
    }
    showImage(imagePath);
}

function parseNumberFromCell(cell) {
    if (!cell) return null;
    let text = '';
    const presetText = cell.querySelector('.preset-text');
    if (presetText) {
        text = presetText.textContent || '';
    } else {
        text = cell.textContent || '';
    }
    const cleaned = text.replace(/[^0-9.\-]/g, '');
    if (!cleaned) return null;
    const num = Number(cleaned);
    return Number.isFinite(num) ? num : null;
}

function parseNumber(value) {
    if (value === null || value === undefined) return null;
    if (typeof value === 'number') return Number.isFinite(value) ? value : null;
    const cleaned = String(value).replace(/[^0-9.\-]/g, '');
    if (!cleaned) return null;
    const num = Number(cleaned);
    return Number.isFinite(num) ? num : null;
}

function updateSelectionStats() {
    const statsEl = document.getElementById('selection-stats');
    if (!statsEl) return;

    if (!selectedCells || selectedCells.length <= 1) {
        statsEl.style.display = 'none';
        return;
    }

    const numbers = [];
    selectedCells.forEach(cell => {
        if (!cell || cell.closest('.totals-row')) return;
        const val = parseNumberFromCell(cell);
        if (val !== null) numbers.push(val);
    });

    const count = selectedCells.length;
    const numCount = numbers.length;
    const sum = numCount ? numbers.reduce((a, b) => a + b, 0) : 0;
    const avg = numCount ? sum / numCount : 0;
    const min = numCount ? Math.min(...numbers) : 0;
    const max = numCount ? Math.max(...numbers) : 0;

    const sumEl = document.getElementById('stat-sum');
    const avgEl = document.getElementById('stat-avg');
    const minEl = document.getElementById('stat-min');
    const maxEl = document.getElementById('stat-max');
    const countEl = document.getElementById('stat-count');
    const numCountEl = document.getElementById('stat-count-num');

    if (sumEl) sumEl.textContent = sum.toFixed(2).replace(/\.00$/, '');
    if (avgEl) avgEl.textContent = avg.toFixed(2).replace(/\.00$/, '');
    if (minEl) minEl.textContent = min.toFixed(2).replace(/\.00$/, '');
    if (maxEl) maxEl.textContent = max.toFixed(2).replace(/\.00$/, '');
    if (countEl) countEl.textContent = String(count);
    if (numCountEl) numCountEl.textContent = String(numCount);

    statsEl.style.display = 'block';
}

// Image size slider
function initImageSizeSlider() {
    const slider = document.getElementById('image-size-slider');
    const display = document.getElementById('image-size-value');
    
    if (slider && display) {
        // Load saved size or use default
        const savedSize = localStorage.getItem('imageSize') || '180';
        slider.value = savedSize;
        display.textContent = savedSize + 'px';
        document.documentElement.style.setProperty('--image-size', savedSize + 'px');
        
        slider.addEventListener('input', function() {
            const size = this.value;
            display.textContent = size + 'px';
            document.documentElement.style.setProperty('--image-size', size + 'px');
            localStorage.setItem('imageSize', size);
        });
    }
}

function initFontSizeSlider() {
    const slider = document.getElementById('font-size-slider');
    const display = document.getElementById('font-size-value');
    
    if (slider && display) {
        const savedSize = localStorage.getItem('tableFontSize') || '14';
        slider.value = savedSize;
        display.textContent = savedSize + 'px';
        document.documentElement.style.setProperty('--table-font-size', savedSize + 'px');
        
        slider.addEventListener('input', function() {
            const size = this.value;
            display.textContent = size + 'px';
            document.documentElement.style.setProperty('--table-font-size', size + 'px');
            localStorage.setItem('tableFontSize', size);
        });
    }
}

function toggleSidebar() {
    const sidebar = document.getElementById('sidebar');
    const toggle = document.getElementById('sidebar-toggle');
    if (!sidebar || !toggle) return;
    const collapsed = sidebar.classList.toggle('collapsed');
    toggle.textContent = '🔧';
    document.body.classList.toggle('sidebar-collapsed', collapsed);
    localStorage.setItem('sidebarCollapsed', collapsed ? 'true' : 'false');
}

function initSidebarState() {
    const sidebar = document.getElementById('sidebar');
    const toggle = document.getElementById('sidebar-toggle');
    if (!sidebar || !toggle) return;
    const collapsed = localStorage.getItem('sidebarCollapsed') === 'true';
    if (collapsed) {
        sidebar.classList.add('collapsed');
        toggle.textContent = '🔧';
        document.body.classList.add('sidebar-collapsed');
    }
}

function initFileMenu() {
    const menu = document.getElementById('file-menu-panel');
    const button = document.getElementById('file-menu-button');
    if (!menu || !button) return;
    button.addEventListener('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
        menu.style.display = menu.style.display === 'block' ? 'none' : 'block';
    });
    document.addEventListener('click', function(e) {
        const container = document.getElementById('file-menu');
        if (!container || !menu) return;
        if (!container.contains(e.target)) {
            menu.style.display = 'none';
        }
    });
}

function initSearch() {
    const searchInput = document.getElementById('search-input');
    if (!searchInput) return;
    let debounceTimer;
    searchInput.addEventListener('input', function() {
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(() => {
            searchQuery = searchInput.value.trim();
            renderTable();
        }, 200);
    });
    // Ctrl/Cmd+F focuses search
    document.addEventListener('keydown', function(e) {
        if ((e.ctrlKey || e.metaKey) && e.key === 'f') {
            // Only intercept if not editing a cell
            if (!isEditingCell && document.activeElement?.contentEditable !== 'true') {
                e.preventDefault();
                searchInput.focus();
                searchInput.select();
            }
        }
    });
}

function setTableBold(isBold) {
    const table = document.querySelector('table.data-table');
    const toggle = document.getElementById('table-bold-toggle');
    if (table) {
        table.classList.toggle('table-bold', isBold);
    }
    if (toggle) {
        toggle.textContent = 'Bold';
        toggle.classList.toggle('active', isBold);
    }
}

function toggleTableBold() {
    const isBold = localStorage.getItem('tableBold') === 'true';
    const next = !isBold;
    localStorage.setItem('tableBold', next.toString());
    setTableBold(next);
}

// Update tfoot totals row structure
function updateTfootTotals() {
    // Don't run if columnOrder isn't defined yet
    if (!columnOrder || columnOrder.length === 0) {
        return;
    }
    
    // Get visible columns
    const visibleColumns = getVisibleColumns();
    
    let tfoot = document.querySelector('tfoot');
    if (!tfoot) {
        // Create tfoot if it doesn't exist
        const table = document.querySelector('table');
        if (table) {
            tfoot = document.createElement('tfoot');
            table.appendChild(tfoot);
        }
    }
    
    if (tfoot) {
        // Always rebuild the row to match current visible columns
        let totalsRowBottom = '';
        for (let i = 0; i < visibleColumns.length; i++) {
            const colId = visibleColumns[i];
            if (colId === 'sold_price') {
                totalsRowBottom += `<td><strong>Totals:</strong></td>`;
            } else if (colId === 'net_revenue') {
                totalsRowBottom += `<td id="total-net-revenue-bottom">$0.00</td>`;
            } else if (colId === 'cost') {
                totalsRowBottom += `<td id="total-cost-bottom">$0.00</td>`;
            } else if (colId === 'profit') {
                totalsRowBottom += `<td id="total-profit-bottom">$0.00</td>`;
            } else {
                totalsRowBottom += `<td></td>`;
            }
        }
        
        let tfootRow = tfoot.querySelector('tr');
        if (!tfootRow) {
            tfootRow = document.createElement('tr');
            tfootRow.className = 'totals-row';
            tfootRow.id = 'totals-row-bottom';
            tfoot.appendChild(tfootRow);
        }
        tfootRow.innerHTML = totalsRowBottom;
    }
}

// Calculate and update totals
function updateTotals() {
    const commissionRate = parseFloat(document.getElementById('commission-rate').value) / 100;
    let totalNetRevenue = 0;
    let totalCost = 0;
    let totalProfit = 0;
    
    const visibleItems = Array.isArray(displayItems) && displayItems.length ? displayItems : items;
    const sourceItems = (selectedRowIndices && selectedRowIndices.size > 0)
        ? visibleItems.filter((_, idx) => selectedRowIndices.has(idx))
        : visibleItems;

    sourceItems.forEach(item => {
        const parseNumber = (val) => {
            if (val === null || val === undefined) return null;
            if (typeof val === 'number') return Number.isFinite(val) ? val : null;
            const cleaned = String(val).replace(/[^0-9.\-]/g, '');
            if (!cleaned) return null;
            const num = Number(cleaned);
            return Number.isFinite(num) ? num : null;
        };

        const soldPriceVal = item.sold_price_float !== null && item.sold_price_float !== undefined
            ? item.sold_price_float
            : parseNumber(item.sold_price);
        const cost = parseNumber(item.cost) || 0;
        const netRevenue = calcNetRevenue(soldPriceVal, item.preset_name);
        const profit = netRevenue !== null ? netRevenue - cost : null;
        
        if (netRevenue !== null) totalNetRevenue += netRevenue;
        totalCost += cost || 0;
        if (profit !== null) totalProfit += profit;
    });
    
    // Update bottom totals (ensure tfoot exists first)
    updateTfootTotals();
    // Now update the values (tfoot row exists, just update the cells)
    const bottomNetRev = document.getElementById('total-net-revenue-bottom');
    const bottomCost = document.getElementById('total-cost-bottom');
    const bottomProfit = document.getElementById('total-profit-bottom');
    if (bottomNetRev) {
        bottomNetRev.textContent = '$' + totalNetRevenue.toFixed(2);
    }
    if (bottomCost) {
        bottomCost.textContent = '$' + totalCost.toFixed(2);
    }
    if (bottomProfit) {
        bottomProfit.textContent = '$' + totalProfit.toFixed(2);
    }
}

// Update net revenue and profit cells for a single row
function updateRowCalculations(row, item) {
    if (!row || !item) return;

    const netRevenue = calcNetRevenue(item.sold_price_float, item.preset_name);
    const netRevenueCell = row.querySelector('.editable-net-revenue');
    if (netRevenueCell) {
        netRevenueCell.textContent = netRevenue !== null ? '$' + netRevenue.toFixed(2) : 'N/A';
    }

    const profit = calcProfit(item.sold_price_float, item.cost, item.preset_name);
    const profitCell = row.querySelector('.profit');
    if (profitCell) {
        const profitClass = profit !== null ? (profit >= 0 ? 'profit-positive' : 'profit-negative') : '';
        profitCell.textContent = profit !== null ? '$' + profit.toFixed(2) : 'N/A';
        profitCell.className = `profit ${profitClass}`;
        profitCell.style.color = profitClass.includes('positive') ? '#28a745' : profitClass.includes('negative') ? '#dc3545' : '';
    }
}

// Undo/Redo functionality
let undoStack = [];
let redoStack = [];
const MAX_UNDO_HISTORY = 50;

function saveState(meta = null) {
    // Deep clone the items array
    const state = JSON.parse(JSON.stringify(items));
    undoStack.push({ items: state, meta });
    // Limit stack size
    if (undoStack.length > MAX_UNDO_HISTORY) {
        undoStack.shift();
    }
    // Clear redo stack when new action is performed
    redoStack = [];
}

async function undo() {
    if (undoStack.length === 0) {
        showToast('Nothing to undo', 'info', 1500);
        return;
    }

    // Save current state to redo stack
    const currentState = JSON.parse(JSON.stringify(items));
    redoStack.push({ items: currentState, meta: null });

    // Restore previous state
    const previousState = undoStack.pop();
    const restoredItems = Array.isArray(previousState)
        ? previousState
        : previousState.items;
    const meta = Array.isArray(previousState) ? null : previousState.meta;
    if (meta && meta.deletedRows && meta.deletedRows.length > 0) {
        try {
            await fetch('/api/items/restore', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    show_id: currentShowId,
                    rows: meta.deletedRows
                })
            });
        } catch (error) {
            console.error('Error restoring deleted rows:', error);
        }
    }
    items = JSON.parse(JSON.stringify(restoredItems));

    // Reload the table
    renderTable();
    updateTotals();
    showToast('Undone', 'success', 1500);
}

function redo() {
    if (redoStack.length === 0) {
        showToast('Nothing to redo', 'info', 1500);
        return;
    }

    // Save current state to undo stack
    const currentState = JSON.parse(JSON.stringify(items));
    undoStack.push({ items: currentState, meta: null });

    // Restore next state
    const nextState = redoStack.pop();
    const restoredItems = Array.isArray(nextState) ? nextState : nextState.items;
    items = JSON.parse(JSON.stringify(restoredItems));

    // Reload the table
    renderTable();
    updateTotals();
    showToast('Redone', 'success', 1500);
}

// Spreadsheet selection with copy/paste
let selectedCells = [];
let isSelecting = false;
let selectionStart = null;
let selectionAnchor = null; // Anchor point for Shift+Click
let clipboard = [];
let clipboardData = null;
let clipboardRows = 0;
let clipboardCols = 0;

// --- Spreadsheet Selection Utilities ---

function getCellTextForClipboard(cell) {
    if (!cell) return '';
    if (cell.classList.contains('image-cell')) {
        const img = cell.querySelector('img');
        return img ? img.src : '';
    }
    if (cell.classList.contains('editable-preset-cell')) {
        const presetDisplay = cell.querySelector('.preset-text');
        return presetDisplay ? presetDisplay.textContent.trim() : '';
    }
    const skuText = cell.querySelector('.sku-text');
    if (skuText) return skuText.textContent.trim();
    const select = cell.querySelector('.cancelled-status-select');
    if (select) return select.value || '';
    const textNodes = Array.from(cell.childNodes).filter(node => node.nodeType === Node.TEXT_NODE);
    const text = textNodes.map(node => node.textContent).join('').trim();
    return text || cell.textContent.replace(/\bCM\b/g, '').trim();
}

// Clear all selections
function clearSelections() {
    document.querySelectorAll('.selected, .selecting, .cell-selection-start').forEach(el => {
        el.classList.remove('selected', 'selecting', 'cell-selection-start');
    });
    selectedCells = [];
    updateSelectionStats();
}

// Get cell coordinates (relative to data rows, excluding totals row)
function getCellCoords(cell) {
    const row = cell.parentElement;
    const tbody = row.parentElement;
    const rows = Array.from(tbody.children);

    // Find the actual row index, skipping totals row
    let rowIndex = rows.indexOf(row);
    if (row.classList.contains('totals-row')) {
        return null; // Don't allow selection of totals row
    }

    // Count how many totals rows are before this row
    let totalsRowCount = 0;
    for (let i = 0; i < rowIndex; i++) {
        if (rows[i].classList.contains('totals-row')) {
            totalsRowCount++;
        }
    }

    // Subtract totals rows to get data row index
    const dataRowIndex = rowIndex - totalsRowCount;
    const cellIndex = Array.from(row.children).indexOf(cell);
    return { row: dataRowIndex, col: cellIndex };
}

// Get cell at coordinates (data row coordinates)
function getCellAtCoords(rowIndex, colIndex) {
    const tbody = document.querySelector('tbody');
    if (!tbody) return null;
    const rows = Array.from(tbody.children);

    // Skip totals rows and find the data row
    let dataRowCount = 0;
    for (let i = 0; i < rows.length; i++) {
        if (!rows[i].classList.contains('totals-row')) {
            if (dataRowCount === rowIndex) {
                const cells = Array.from(rows[i].children);
                if (colIndex >= 0 && colIndex < cells.length) {
                    return cells[colIndex];
                }
            }
            dataRowCount++;
        }
    }
    return null;
}

// Select range of cells
function selectRange(start, end, clear = true) {
    if (clear) {
        clearSelections();
    }
    const startRow = Math.min(start.row, end.row);
    const endRow = Math.max(start.row, end.row);
    const startCol = Math.min(start.col, end.col);
    const endCol = Math.max(start.col, end.col);

    const newSelectedCells = [];
    for (let r = startRow; r <= endRow; r++) {
        for (let c = startCol; c <= endCol; c++) {
            const cell = getCellAtCoords(r, c);
            if (cell && !cell.closest('.totals-row')) {
                cell.classList.add('selected');
                if (r === startRow && c === startCol) {
                    cell.classList.add('cell-selection-start');
                }
                newSelectedCells.push(cell);
            }
        }
    }
    if (clear) {
        selectedCells = newSelectedCells;
    } else {
        // Merge selections
        newSelectedCells.forEach(cell => {
            if (!selectedCells.includes(cell)) {
                selectedCells.push(cell);
            }
        });
    }
    updateSelectionStats();
}

function getSelectedCellElements() {
    const found = new Set();
    if (selectedCells && selectedCells.length > 0) {
        selectedCells.forEach(cell => {
            if (cell) found.add(cell);
        });
    }
    document.querySelectorAll('td.selected, td.selecting, td.cell-selection-start').forEach(cell => {
        found.add(cell);
    });
    const selected = Array.from(found);
    if (selected.length > 0) {
        return selected;
    }
    if (document.activeElement) {
        const activeCell = document.activeElement.closest('td');
        if (activeCell) {
            return [activeCell];
        }
    }
    return [];
}

function clearSelectedCellsBatch() {
    const selectedElements = getSelectedCellElements();
    if (!selectedElements || selectedElements.length === 0) {
        return;
    }

    const cellsSnapshot = selectedElements.slice();
    const cellsToClear = [];
    const clearableFields = new Set([
        'sold_price',
        'cost',
        'sku',
        'notes',
        'buyer',
        'order_id',
        'preset',
        'preset_display'
    ]);

    cellsSnapshot.forEach(cell => {
        if (!cell || cell.closest('.totals-row')) return;
        const itemName = cell.getAttribute('data-item-name');
        let field = cell.getAttribute('data-field');
        if (!field) {
            const fieldEl = cell.querySelector('[data-field]');
            field = fieldEl ? fieldEl.getAttribute('data-field') : null;
        }
        if (itemName && field && clearableFields.has(field)) {
            cellsToClear.push({ cell, itemName, field });
            return;
        }
        if (cell.contentEditable === 'true') {
            cell.textContent = '';
        }
    });

    if (cellsToClear.length === 0) {
        clearSelections();
        return;
    }

    // Save state for undo
    saveState();

    // Prevent blur handlers from firing while we update
    isBatchPasting = true;

    // Clear UI first
    cellsToClear.forEach(item => {
        if (item.field === 'sku') {
            const skuText = item.cell.querySelector('.sku-text');
            if (skuText) {
                skuText.textContent = '';
            }
            const skuSelect = item.cell.querySelector('.sku-select');
            if (skuSelect) {
                skuSelect.value = '';
            }
        } else if (item.field === 'preset' || item.field === 'preset_display') {
            const presetText = item.cell.querySelector('.preset-text');
            if (presetText) {
                presetText.textContent = '';
            }
            const presetToggle = item.cell.querySelector('.preset-select-toggle');
            if (presetToggle) {
                presetToggle.textContent = 'Select preset...';
                presetToggle.setAttribute('data-value', '');
            }
        } else if (item.cell.contentEditable === 'true') {
            item.cell.textContent = '';
        }
    });

    // Blur any active cell to avoid conflicts
    const activeElement = document.activeElement;
    if (activeElement && activeElement.contentEditable === 'true') {
        activeElement.blur();
    }

    const apiPromises = cellsToClear.map(item => {
        const { itemName, field } = item;

        if (field === 'net_revenue' || field === 'profit') {
            return Promise.resolve();
        }

        if (field === 'sold_price') {
            return fetch('/api/items/sold-price', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    item_name: itemName,
                    sold_price: '',
                    show_id: currentShowId
                })
            });
        }

        if (field === 'cost') {
            return fetch('/api/items/cost', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    item_names: [itemName],
                    cost: null,
                    show_id: currentShowId
                })
            });
        }

        if (field === 'preset' || field === 'preset_display') {
            return fetch('/api/items/cost', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    item_names: [itemName],
                    cost: null,
                    preset_name: null,
                    show_id: currentShowId
                })
            });
        }

        if (field === 'sku') {
            return fetch('/api/items/sku', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    item_name: itemName,
                    sku: '',
                    show_id: currentShowId
                })
            });
        }

        if (field === 'notes') {
            return fetch('/api/items/notes', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    item_name: itemName,
                    notes: '',
                    show_id: currentShowId
                })
            });
        }

        if (field === 'buyer') {
            return fetch('/api/items/buyer', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    item_name: itemName,
                    buyer: '',
                    show_id: currentShowId
                })
            });
        }

        if (field === 'order_id') {
            return fetch('/api/items/order-id', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    item_name: itemName,
                    order_id: '',
                    show_id: currentShowId
                })
            });
        }

        return Promise.resolve();
    });

    Promise.all(apiPromises).then(() => {
        isBatchPasting = false;

        cellsToClear.forEach(item => {
            const matchingItem = items.find(i => i.item_name === item.itemName);
            if (!matchingItem) return;

            if (item.field === 'sold_price') {
                matchingItem.sold_price = '';
                matchingItem.sold_price_float = null;
            } else if (item.field === 'cost') {
                matchingItem.cost = null;
            } else if (item.field === 'sku') {
                matchingItem.sku = null;
            } else if (item.field === 'preset' || item.field === 'preset_display') {
                matchingItem.preset_name = null;
                matchingItem.cost = null;
            } else if (item.field === 'notes') {
                matchingItem.notes = null;
            } else if (item.field === 'buyer') {
                matchingItem.buyer = null;
            } else if (item.field === 'order_id') {
                matchingItem.order_id = null;
            }
        });

        const updatedRows = new Set();
        cellsToClear.forEach(item => {
            if (!item.cell) return;
            const row = item.cell.closest('tr');
            if (!row || updatedRows.has(row)) return;
            const matchingItem = items.find(i => i.item_name === item.itemName);
            if (matchingItem) {
                updateRowCalculations(row, matchingItem);
                updatedRows.add(row);
            }
        });

        updateTotals();
    }).catch(error => {
        console.error('Error clearing selected cells:', error);
        isBatchPasting = false;
        loadData();
    });

    clearSelections();
}

function clearSingleSelectedCell() {
    const selectedElements = getSelectedCellElements();
    const targetCell = selectedElements.length > 0
        ? selectedElements[selectedElements.length - 1]
        : null;
    if (!targetCell || targetCell.closest('.totals-row')) {
        return;
    }
    selectedCells = [targetCell];
    clearSelectedCellsBatch();
}

// Sync pending edits helper
function syncPendingEdits() {
    pendingEdits.forEach((value, key) => {
        const [itemKey, field] = key.split('::');
        const itemName = decodeURIComponent(itemKey || '');
        updateItemField(itemName, field, value);
    });
}

// Helper function to paste clipboard data
function pasteClipboard(startCell) {
    // If multiple cells are selected, paste into all selected cells
    if (selectedCells && selectedCells.length > 1 && clipboard && clipboard.length > 0) {
        // Create a snapshot: copy selectedCells array immediately
        const cellsSnapshot = selectedCells.slice(); // Create a copy of the array

        // Build the cellsToPaste array with all data we need
        const cellsToPaste = [];
        for (let i = 0; i < cellsSnapshot.length; i++) {
            const cell = cellsSnapshot[i];
            // Skip if cell is invalid or in totals row
            if (!cell || !cell.parentElement || cell.closest('.totals-row')) {
                continue;
            }

            // Handle preset cells specially
            if (cell.classList.contains('editable-preset-cell')) {
                const index = cell.getAttribute('data-index');
                if (index !== null) {
                    cellsToPaste.push({
                        cell: cell,
                        index: parseInt(index),
                        isPreset: true,
                        pasteIndex: i
                    });
                }
                continue;
            }

            const itemName = cell.getAttribute('data-item-name');
            const field = cell.getAttribute('data-field');

            const skuText = cell.querySelector('.sku-text');
            const isImageCell = field === 'image' && cell.classList.contains('image-cell');
            const isEditableCell = cell.contentEditable === 'true' || (skuText && skuText.contentEditable === 'true') || isImageCell;
            // Only process editable cells with required attributes
            if (itemName && field && isEditableCell) {
                cellsToPaste.push({
                    cell: cell,
                    itemName: itemName,
                    field: field,
                    isPreset: false,
                    pasteIndex: i
                });
            }
        }

        if (cellsToPaste.length === 0) return;

        // Save state for undo
        saveState();

        // CRITICAL: Set batch paste flag FIRST, before any DOM changes
        isBatchPasting = true;

        // Separate preset cells from regular cells
        const presetCellsToPaste = cellsToPaste.filter(item => item.isPreset);
        const regularCellsToPaste = cellsToPaste.filter(item => !item.isPreset);

        // Update ALL regular cells synchronously FIRST - before any blur events
        for (let i = 0; i < regularCellsToPaste.length; i++) {
            const item = regularCellsToPaste[i];
            const clipboardIndex = item.pasteIndex % clipboard.length;
            const value = clipboard[clipboardIndex] || '';
            const targetEl = item.cell.querySelector('.sku-text') || item.cell;
            targetEl.textContent = value;
            item.newValue = value;
            const itemKey = item.cell.getAttribute('data-item-key');
            if (itemKey && item.field) {
                pendingEdits.set(`${itemKey}::${item.field}`, value);
            }
            updateItemField(item.itemName, item.field, value);
        }

        // Handle preset cells - find matching preset and apply
        for (let i = 0; i < presetCellsToPaste.length; i++) {
            const item = presetCellsToPaste[i];
            const clipboardIndex = item.pasteIndex % clipboard.length;
            const presetName = (clipboard[clipboardIndex] || '').trim();

            if (presetName) {
                // Find matching preset
                const matchingPreset = presets.find(p => p.name === presetName);
                if (matchingPreset) {
                    // Apply preset to this row
                    applyPresetToRow(item.index, matchingPreset.name, matchingPreset.cost);
                }
            }
        }

        // Blur any currently focused cell AFTER updating (prevents interference)
        const activeElement = document.activeElement;
        if (activeElement && activeElement.contentEditable === 'true') {
            activeElement.blur();
        }

        // Batch all API updates for regular cells
        const apiPromises = regularCellsToPaste.map((item) => {
            const { itemName, field, newValue } = item;

            // Skip read-only fields
            if (field === 'net_revenue' || field === 'profit') {
                return Promise.resolve();
            }

            if (field === 'sold_price') {
                let cleanedValue = newValue.replace(/[^0-9.]/g, '');
                const soldPrice = cleanedValue === '' ? '' : cleanedValue;
                return fetch('/api/items/sold-price', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        item_name: itemName,
                        sold_price: soldPrice,
                        show_id: currentShowId
                    })
                });
            } else if (field === 'image') {
                if (newValue && (newValue.startsWith('http://') || newValue.startsWith('https://') || newValue.startsWith('data:image'))) {
                    return saveImageForItem(itemName, newValue, item.cell);
                }
                return Promise.resolve();
            } else if (field === 'sold_timestamp') {
                return fetch('/api/items/sold-time', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        item_name: itemName,
                        sold_timestamp: newValue || '',
                        show_id: currentShowId
                    })
                });
            } else if (field === 'viewers') {
                return fetch('/api/items/viewers', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        item_name: itemName,
                        viewers: newValue || '',
                        show_id: currentShowId
                    })
                });
            } else if (field === 'cost') {
                // Clean the value: remove $ and other non-numeric characters except decimal point
                let cleanedValue = newValue.replace(/[^0-9.]/g, '');
                let cost = null;
                if (cleanedValue) {
                    cost = parseFloat(cleanedValue);
                    if (isNaN(cost)) return Promise.resolve();
                }
                return fetch('/api/items/cost', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        item_names: [itemName],
                        cost: cost,
                        show_id: currentShowId
                    })
                });
            } else if (field === 'sku') {
                return fetch('/api/items/sku', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        item_name: itemName,
                        sku: newValue || '',
                        show_id: currentShowId
                    })
                });
            }
            return Promise.resolve();
        });

        Promise.all(apiPromises).then((responses) => {
            // Clear batch flag immediately
            isBatchPasting = false;

            // Update the items array to reflect changes (for totals calculation)
            cellsToPaste.forEach((item) => {
                const matchingItem = items.find(i => i.item_name === item.itemName);
            if (matchingItem && item.field === 'sold_price') {
                    const cleanedValue = item.newValue.replace(/[^0-9.]/g, '');
                    matchingItem.sold_price = cleanedValue === '' ? '' : cleanedValue;
                    // Update float for calculations
                    if (cleanedValue) {
                        matchingItem.sold_price_float = parseFloat(cleanedValue);
                    } else {
                        matchingItem.sold_price_float = null;
                    }
            } else if (matchingItem && item.field === 'sold_timestamp') {
                matchingItem.sold_timestamp = item.newValue || '';
            } else if (matchingItem && item.field === 'viewers') {
                matchingItem.viewers = item.newValue || '';
                } else if (matchingItem && item.field === 'cost') {
                    // Clean the value before parsing
                    const cleanedValue = item.newValue.replace(/[^0-9.]/g, '');
                    if (cleanedValue) {
                        matchingItem.cost = parseFloat(cleanedValue);
                    } else {
                        matchingItem.cost = null;
                    }
                }
            });

            // Update net revenue and profit for affected rows
            const updatedRows = new Set();
            cellsToPaste.forEach((item) => {
                if (!item.cell) return;
                const row = item.cell.closest('tr');
                if (!row || updatedRows.has(row)) return;
                const matchingItem = items.find(i => i.item_name === item.itemName);
                if (matchingItem) {
                    updateRowCalculations(row, matchingItem);
                    updatedRows.add(row);
                }
            });

            // Update totals to reflect the changes
            updateTotals();
            // Keep in-memory items in sync to prevent sort wipe
            syncPendingEdits();
        }).catch(error => {
            console.error('Error during batch paste:', error);
            isBatchPasting = false;
            // Only reload on error to restore state
            loadData();
        });

        // Clear selection after paste
        clearSelections();
        return;
    }

    // Single cell or no selection: paste starting from startCell maintaining structure
    if (!startCell || startCell.closest('.totals-row')) return;

    const startCoords = getCellCoords(startCell);
    if (!startCoords) return;

    // Save state for undo (only if pasting multiple cells, single cell will save in blur)
    if (clipboardRows > 1 || clipboardCols > 1) {
        saveState();
    }

    const cellsToUpdate = [];
    for (let r = 0; r < clipboardRows; r++) {
        for (let c = 0; c < clipboardCols; c++) {
            const targetRow = startCoords.row + r;
            const targetCol = startCoords.col + c;

            const targetCell = getCellAtCoords(targetRow, targetCol);
            if (targetCell && !targetCell.closest('.totals-row')) {
                const value = clipboardData[r][c];

                // Handle preset cells specially
                if (targetCell.classList.contains('editable-preset-cell')) {
                    const index = targetCell.getAttribute('data-index');
                    if (index !== null && value.trim()) {
                        // Find matching preset
                        const matchingPreset = presets.find(p => p.name === value.trim());
                        if (matchingPreset) {
                            applyPresetToRow(parseInt(index), matchingPreset.name, matchingPreset.cost);
                        }
                    }
                } else {
                    const skuText = targetCell.querySelector('.sku-text');
                    if (skuText && skuText.contentEditable === 'true') {
                        skuText.textContent = value;
                        cellsToUpdate.push(skuText);
                        const itemKey = skuText.getAttribute('data-item-key');
                        const field = skuText.getAttribute('data-field');
                        const itemName = skuText.getAttribute('data-item-name');
                        if (itemKey && field) {
                            pendingEdits.set(`${itemKey}::${field}`, value);
                        }
                        updateItemField(itemName, field, value);
                    } else if (targetCell.contentEditable === 'true') {
                        targetCell.textContent = value;
                        cellsToUpdate.push(targetCell);
                        const itemKey = targetCell.getAttribute('data-item-key');
                        const field = targetCell.getAttribute('data-field');
                        const itemName = targetCell.getAttribute('data-item-name');
                        if (itemKey && field) {
                            pendingEdits.set(`${itemKey}::${field}`, value);
                        }
                        updateItemField(itemName, field, value);
                    }
                }
            }
        }
    }

    // Trigger blur events after all updates
    cellsToUpdate.forEach(cell => {
        cell.dispatchEvent(new Event('blur', { bubbles: true }));
    });
    syncPendingEdits();

    // Clear selection after paste
    clearSelections();
}

// --- Spreadsheet Selection Event Handlers ---

function handleSelectionMouseDown(e) {
    // Don't interfere with buttons, images, selects, resize handles, etc.
    // But allow clicks on contenteditable cells for editing
    if (e.target.tagName === 'BUTTON' ||
        e.target.tagName === 'SELECT' || e.target.tagName === 'IMG' ||
        e.target.tagName === 'A' || e.target.closest('.tooltip') ||
        e.target.closest('.resize-border') ||
        e.target.closest('th')) {
        return;
    }

    // If clicking on input (preset inputs), don't interfere
    if (e.target.tagName === 'INPUT' && e.target.closest('.preset-cell')) {
        return;
    }

    // If clicking on preset text, select the parent preset cell
    if (e.target.classList.contains('preset-text')) {
        const cell = e.target.closest('.editable-preset-cell');
        if (cell) {
            e.preventDefault();
            isSelecting = true;
            const coords = getCellCoords(cell);
            selectionStart = coords;
            clearSelections();
            cell.classList.add('selected', 'cell-selection-start');
            selectedCells = [cell];
            selectionAnchor = coords;
        }
        return;
    }

    const skuCell = e.target.closest('td');
    const skuText = skuCell ? skuCell.querySelector('.sku-text') : null;
    if (skuText && !e.target.closest('select')) {
        e.preventDefault();
        const coords = getCellCoords(skuCell);
        if (e.shiftKey && selectionAnchor && coords) {
            selectRange(selectionAnchor, coords);
        } else if (coords) {
            isSelecting = true;
            selectionStart = coords;
            clearSelections();
            skuCell.classList.add('selected', 'cell-selection-start');
            selectedCells = [skuCell];
            selectionAnchor = coords;
            updateSelectionStats();
        }
        skuText.focus();
        return;
    }

    const cell = e.target.closest('td');
    if (cell && !cell.closest('.totals-row')) {
        // Spreadsheet-style editing: focus editable cells on click
        if ((cell.contentEditable === 'true' || e.target.closest('[contenteditable="true"]')) && !e.shiftKey && !e.metaKey && !e.ctrlKey) {
            clearSelections();
            cell.classList.add('selected', 'cell-selection-start');
            selectedCells = [cell];
            selectionAnchor = getCellCoords(cell);
            cell.focus();
            return;
        }
        if (
            presetFilterSelected &&
            cell.contentEditable === 'true' &&
            !e.shiftKey &&
            !e.metaKey &&
            !e.ctrlKey
        ) {
            return;
        }
        e.preventDefault();
        isSelecting = true;
        const coords = getCellCoords(cell);
        selectionStart = coords;

        // If Shift is pressed, extend from anchor
        if (e.shiftKey && selectionAnchor) {
            selectRange(selectionAnchor, coords);
        } else {
            // New selection
            clearSelections();
            cell.classList.add('selected', 'cell-selection-start');
            selectedCells = [cell];
            selectionAnchor = coords;
            updateSelectionStats();
        }
    }
}

function handleSelectionMouseMove(e) {
    if (!isSelecting || !selectionStart) return;

    const cell = e.target.closest('td');
    if (cell && !cell.closest('.totals-row')) {
        const current = getCellCoords(cell);
        selectRange(selectionStart, current);
    }
}

function handleSelectionMouseUp(e) {
    if (isSelecting) {
        isSelecting = false;
        // Remove 'selecting' class, keep 'selected'
        document.querySelectorAll('.selecting').forEach(el => {
            el.classList.remove('selecting');
        });
    }
}

function handleSelectionClick(e) {
    const cmButton = e.target.closest('.cm-icon');
    if (cmButton) {
        e.preventDefault();
        e.stopPropagation();
        const cell = cmButton.closest('td');
        if (clickModeActive) {
            setClickMode(false);
            return;
        }
        const value = getCellValueForClickMode(cell);
        if (!value) return;
        const input = document.getElementById('click-mode-input');
        if (input) input.value = value;
        setClickMode(true, value);
        return;
    }

    if (clickModeActive) {
        const img = e.target.closest('img');
        if (img) {
            e.preventDefault();
            e.stopPropagation();
            const row = img.closest('tr');
            applyClickModeToRow(row, clickModeValue);
            return;
        }

        const presetCell = e.target.closest('.editable-preset-cell');
        const presetDisplay = e.target.closest('.preset-text');
        if (presetDisplay) {
            e.preventDefault();
            e.stopPropagation();
            applyClickModeToCell(presetDisplay.closest('td'), clickModeValue);
            return;
        }
        if (presetCell) {
            e.preventDefault();
            e.stopPropagation();
            applyClickModeToCell(presetCell, clickModeValue);
            return;
        }

        const cancelledSelect = e.target.closest('.cancelled-status-select') || e.target.closest('td')?.querySelector('.cancelled-status-select');
        if (cancelledSelect) {
            e.preventDefault();
            e.stopPropagation();
            applyClickModeToCell(cancelledSelect.closest('td'), clickModeValue);
            return;
        }

        const cell = e.target.closest('td');
        if (cell && cell.querySelector('.sku-text')) {
            e.preventDefault();
            e.stopPropagation();
            applyClickModeToCell(cell, clickModeValue);
            return;
        }
        if (cell && cell.contentEditable === 'true' && !cell.closest('.totals-row') && isClickModeAllowed(cell)) {
            e.preventDefault();
            e.stopPropagation();
            applyClickModeToCell(cell, clickModeValue);
            return;
        }
    }

    // Don't interfere with buttons, selects, images, resize handles, or preset inputs
    if (e.target.tagName === 'BUTTON' ||
        e.target.tagName === 'SELECT' || e.target.tagName === 'IMG' ||
        e.target.closest('.tooltip') ||
        e.target.closest('.resize-border') ||
        (e.target.tagName === 'INPUT' && e.target.closest('.preset-cell'))) {
        return;
    }

    // If clicking anywhere in a preset cell, focus the preset text
    const presetCell = e.target.closest('.editable-preset-cell');
    if (presetCell && !e.target.classList.contains('preset-text')) {
        if (clickModeActive) {
            return;
        }
        const presetText = presetCell.querySelector('.preset-text');
        if (presetText) {
            presetText.focus();
            const range = document.createRange();
            range.selectNodeContents(presetText);
            const sel = window.getSelection();
            sel.removeAllRanges();
            sel.addRange(range);
            return;
        }
    }

    // If clicking on preset text, select the parent preset cell
    if (e.target.classList.contains('preset-text')) {
        const cell = e.target.closest('.editable-preset-cell');
        if (cell && !cell.closest('.totals-row') && !isSelecting) {
            const coords = getCellCoords(cell);
            if (!coords) return;

            clearSelections();
            cell.classList.add('selected', 'cell-selection-start');
            selectedCells = [cell];
            selectionAnchor = coords;
            updateSelectionStats();

            // Focus the preset text for editing
            e.target.focus();
            const range = document.createRange();
            range.selectNodeContents(e.target);
            const sel = window.getSelection();
            sel.removeAllRanges();
            sel.addRange(range);
        }
        return;
    }

    const cell = e.target.closest('td');
    if (!cell || cell.closest('.totals-row') || isSelecting) {
        return;
    }

    // Check if it's a contenteditable cell
    const isEditableCell = cell.contentEditable === 'true';
    const coords = getCellCoords(cell);
    if (!coords) return;

    // Shift+Click: extend selection from anchor
    if (e.shiftKey && selectionAnchor) {
        e.preventDefault();
        selectRange(selectionAnchor, coords);
        return;
    }

    // Normal click on editable cell: select and focus for immediate editing
    if (isEditableCell) {
        clearSelections();
        cell.classList.add('selected', 'cell-selection-start');
        selectedCells = [cell];
        selectionAnchor = coords;
        updateSelectionStats();

        // Focus the cell for immediate editing
        cell.focus();
        // Select all text for easy replacement
        const range = document.createRange();
        range.selectNodeContents(cell);
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);
        return;
    }

    // Normal click on non-editable cell: just select
    clearSelections();
    cell.classList.add('selected', 'cell-selection-start');
    selectedCells = [cell];
    selectionAnchor = coords;
    updateSelectionStats();
}

// Keyboard navigation and shortcuts
function handleSelectionKeyDown(e) {
    const isModifier = e.ctrlKey || e.metaKey;
    const isCopy = isModifier && (e.key === 'c' || e.key === 'C');
    const isPaste = isModifier && (e.key === 'v' || e.key === 'V');
    const isUndo = isModifier && !e.shiftKey && (e.key === 'z' || e.key === 'Z');
    const isRedo = (isModifier && e.shiftKey && (e.key === 'z' || e.key === 'Z')) ||
                  (isModifier && (e.key === 'y' || e.key === 'Y'));
    const isDeleteKey = e.key === 'Delete' || e.key === 'Backspace';
    const isShiftDelete = isDeleteKey && e.shiftKey;

    // Don't interfere with inputs, textareas, or other form elements
    if (e.target.tagName === 'INPUT' && !e.target.closest('.preset-cell')) {
        return;
    }
    if (e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') {
        return;
    }

    // Shift+Delete: clear all selected cells
    if (isShiftDelete && selectedCells && selectedCells.length > 0) {
        e.preventDefault();
        e.stopPropagation();
        clearSelectedCellsBatch();
        return;
    }

    // Delete/Backspace: delete selected rows if row numbers were clicked
    if (
        isDeleteKey &&
        (
            (selectedRowIndices && selectedRowIndices.size > 0) ||
            (lastRowNumberClickAt && Date.now() - lastRowNumberClickAt < 5000)
        )
    ) {
        e.preventDefault();
        e.stopPropagation();
        deleteSelected();
        return;
    }

    // Delete/Backspace: clear only one selected cell
    if (isDeleteKey && selectedCells && selectedCells.length > 0) {
        e.preventDefault();
        e.stopPropagation();
        clearSingleSelectedCell();
        return;
    }

    // Undo (Ctrl+Z / Cmd+Z)
    if (isUndo) {
        e.preventDefault();
        e.stopPropagation();
        undo();
        return;
    }

    // Redo (Shift+Ctrl+Z / Shift+Cmd+Z or Ctrl+Y / Cmd+Y)
    if (isRedo) {
        e.preventDefault();
        e.stopPropagation();
        redo();
        return;
    }

    // Copy (Ctrl+C / Cmd+C)
    if (isCopy && selectedCells && selectedCells.length > 0) {
        e.preventDefault();
        e.stopPropagation();

        // Calculate the bounding box of selected cells
        const coords = selectedCells.map(cell => getCellCoords(cell)).filter(c => c !== null);
        if (coords.length === 0) return;

        const minRow = Math.min(...coords.map(c => c.row));
        const maxRow = Math.max(...coords.map(c => c.row));
        const minCol = Math.min(...coords.map(c => c.col));
        const maxCol = Math.max(...coords.map(c => c.col));

        clipboardRows = maxRow - minRow + 1;
        clipboardCols = maxCol - minCol + 1;

        // Create a 2D array to store clipboard data
        clipboardData = [];
        for (let r = 0; r < clipboardRows; r++) {
            clipboardData[r] = [];
            for (let c = 0; c < clipboardCols; c++) {
                const cell = getCellAtCoords(minRow + r, minCol + c);
                if (cell && selectedCells.includes(cell)) {
                    clipboardData[r][c] = getCellTextForClipboard(cell);
                } else {
                    clipboardData[r][c] = '';
                }
            }
        }

        clipboard = clipboardData.flat();
        return;
    }

    // Copy rows when row numbers are selected
    if (isCopy && selectedRowIndices && selectedRowIndices.size > 0 && (!selectedCells || selectedCells.length === 0)) {
        e.preventDefault();
        e.stopPropagation();
        const rows = Array.from(selectedRowIndices).sort((a, b) => a - b);
        const visibleColumns = getVisibleColumns();
        clipboardRows = rows.length;
        clipboardCols = visibleColumns.length;
        clipboardData = [];
        for (let r = 0; r < clipboardRows; r++) {
            clipboardData[r] = [];
            for (let c = 0; c < clipboardCols; c++) {
                const cell = getCellAtCoords(rows[r], c);
                clipboardData[r][c] = cell ? getCellTextForClipboard(cell) : '';
            }
        }
        clipboard = clipboardData.flat();
        return;
    }

    // Paste (Ctrl+V / Cmd+V)
    if (isPaste && clipboardData && clipboardData.length > 0) {
        e.preventDefault();
        e.stopPropagation();

        // Always check for multiple selected cells first
        if (selectedCells && selectedCells.length > 1) {
            pasteClipboard(null);
            return;
        }

        // Paste into selected rows if no cells selected
        if ((!selectedCells || selectedCells.length === 0) && selectedRowIndices && selectedRowIndices.size > 0) {
            const rows = Array.from(selectedRowIndices).sort((a, b) => a - b);
            const startRow = rows[0];
            const startCol = 0;
            selectedCells = [];
            for (let r = 0; r < clipboardRows; r++) {
                for (let c = 0; c < clipboardCols; c++) {
                    const cell = getCellAtCoords(startRow + r, startCol + c);
                    if (cell && !cell.closest('.totals-row')) {
                        selectedCells.push(cell);
                    }
                }
            }
            if (selectedCells.length > 0) {
                pasteClipboard(null);
            }
            return;
        }

        // Single cell paste
        let startCell = selectedCells.length > 0 ? selectedCells[0] : null;
        if (!startCell) {
            const activeElement = document.activeElement;
            if (activeElement && activeElement.closest('td')) {
                startCell = activeElement.closest('td');
            }
        }
        if (startCell) {
            pasteClipboard(startCell);
        }
        return;
    }

    // Arrow key navigation (only when not editing)
    if (!e.target.contentEditable || e.target.contentEditable !== 'true' || document.activeElement !== e.target) {
        const arrowKeys = ['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight'];
        if (arrowKeys.includes(e.key)) {
            const activeCell = document.activeElement.closest('td');
            if (activeCell && !activeCell.closest('.totals-row')) {
                e.preventDefault();
                const coords = getCellCoords(activeCell);
                if (!coords) return;

                let newRow = coords.row;
                let newCol = coords.col;

                if (e.key === 'ArrowUp') newRow = Math.max(0, newRow - 1);
                else if (e.key === 'ArrowDown') newRow++;
                else if (e.key === 'ArrowLeft') newCol = Math.max(0, newCol - 1);
                else if (e.key === 'ArrowRight') newCol++;

                const newCell = getCellAtCoords(newRow, newCol);
                if (newCell && !newCell.closest('.totals-row')) {
                    clearSelections();
                    newCell.classList.add('selected', 'cell-selection-start');
                    selectedCells = [newCell];
                    selectionAnchor = getCellCoords(newCell);
                    newCell.focus();
                }
            }
            return;
        }
    }
}

function handleImagePaste(e) {
    const targetCell = e.target.closest ? e.target.closest('.image-cell') : null;
    if (!targetCell) return;
    const itemName = targetCell.getAttribute('data-item-name');
    if (!itemName) return;
    const clipboard = e.clipboardData;
    if (clipboard && clipboard.items) {
        for (const item of clipboard.items) {
            if (item.type && item.type.startsWith('image/')) {
                e.preventDefault();
                const file = item.getAsFile();
                if (!file) return;
                const reader = new FileReader();
                reader.onload = () => {
                    const dataUrl = reader.result;
                    if (typeof dataUrl === 'string') {
                        saveImageForItem(itemName, dataUrl, targetCell);
                    }
                };
                reader.readAsDataURL(file);
                return;
            }
        }
    }
    const text = clipboard ? clipboard.getData('text/plain') : '';
    if (text && (text.startsWith('http://') || text.startsWith('https://') || text.startsWith('data:image'))) {
        e.preventDefault();
        saveImageForItem(itemName, text.trim(), targetCell);
    }
}

// --- Initialize Spreadsheet Selection (registers event listeners) ---

function initSpreadsheetSelection() {
    document.addEventListener('mousedown', handleSelectionMouseDown);
    document.addEventListener('mousemove', handleSelectionMouseMove);
    document.addEventListener('mouseup', handleSelectionMouseUp);
    document.addEventListener('click', handleSelectionClick);
    document.addEventListener('keydown', handleSelectionKeyDown);
    document.addEventListener('paste', handleImagePaste);
}

// Store user input states before reloading
function saveInputStates() {
    const states = {
        checkboxes: {},
        presetInputs: {},
        soldPriceInputs: {},
        costInputs: {},
        commissionRate: null
    };
    
    // Save selected row indices
    states.selectedRows = Array.from(selectedRowIndices);
    
    // Save preset input states
    document.querySelectorAll('.item-preset-name').forEach(input => {
        const index = input.getAttribute('data-index');
        if (index && input.value.trim()) {
            states.presetInputs[index] = { name: input.value, cost: null };
        }
    });
    
    document.querySelectorAll('.item-preset-cost').forEach(input => {
        const index = input.getAttribute('data-index');
        if (index && input.value.trim()) {
            if (!states.presetInputs[index]) {
                states.presetInputs[index] = { name: '', cost: input.value };
            } else {
                states.presetInputs[index].cost = input.value;
            }
        }
    });
    
    // Save sold price inputs (now contenteditable cells)
    document.querySelectorAll('.editable-sold-price').forEach(cell => {
        const itemName = cell.getAttribute('data-item-name');
        if (itemName && cell.textContent && cell.textContent.trim()) {
            states.soldPriceInputs[itemName] = cell.textContent.trim();
        }
    });
    
    // Save cost inputs (now contenteditable cells)
    document.querySelectorAll('.editable-cost').forEach(cell => {
        const itemName = cell.getAttribute('data-item-name');
        if (itemName && cell.textContent && cell.textContent.trim()) {
            states.costInputs[itemName] = cell.textContent.trim();
        }
    });
    
    // Save commission rate
    const commissionInput = document.getElementById('commission-rate');
    if (commissionInput) {
        states.commissionRate = commissionInput.value;
    }
    
    return states;
}

function restoreInputStates(states) {
    // Restore row selections
    if (states.selectedRows && Array.isArray(states.selectedRows)) {
        selectedRowIndices = new Set(states.selectedRows.map(idx => parseInt(idx)));
        updateRowSelection();
    }
    
    // Restore preset inputs
    Object.keys(states.presetInputs).forEach(index => {
        const preset = states.presetInputs[index];
        const select = document.querySelector(`.item-preset-select[data-index="${index}"]`);
        const nameInput = document.querySelector(`.item-preset-name[data-index="${index}"]`);
        const costInput = document.querySelector(`.item-preset-cost[data-index="${index}"]`);
        const presetDisplay = document.querySelector(`.preset-text[data-index="${index}"]`);
        
        if (select && preset.selectValue) {
            select.value = preset.selectValue;
        }
        if (nameInput && preset.name) {
            nameInput.value = preset.name;
        }
        if (costInput && preset.cost) {
            costInput.value = preset.cost;
        }
        if (presetDisplay && preset.displayValue) {
            presetDisplay.textContent = preset.displayValue;
        }
    });
    
    // Restore sold price cells (contenteditable, not inputs)
    Object.keys(states.soldPriceInputs).forEach(itemName => {
        const cell = document.querySelector(`.editable-sold-price[data-item-name="${escapeHtml(itemName)}"]`);
        if (cell && cell.textContent !== states.soldPriceInputs[itemName]) {
            cell.textContent = states.soldPriceInputs[itemName];
        }
    });
    
    // Restore cost cells (contenteditable, not inputs)
    Object.keys(states.costInputs).forEach(itemName => {
        const cell = document.querySelector(`.editable-cost[data-item-name="${escapeHtml(itemName)}"]`);
        if (cell && cell.textContent !== states.costInputs[itemName]) {
            cell.textContent = states.costInputs[itemName];
        }
    });
    
    // Restore commission rate
    if (states.commissionRate) {
        const commissionInput = document.getElementById('commission-rate');
        if (commissionInput) {
            commissionInput.value = states.commissionRate;
            // Trigger change to recalculate
            commissionInput.dispatchEvent(new Event('change'));
        }
    }
}

// Input state preservation is now built into loadData directly

// Column widths storage (by column ID, not index)
const columnWidths = {};
function getHeaders() {
    return Array.from(document.querySelectorAll('thead tr.header-row th'));
}
const columnResizeState = {
    active: false,
    pending: false,
    moved: false,
    header: null,
    columnId: null,
    startX: 0,
    startWidth: 0
};
const columnDragState = {
    active: false,
    pending: false,
    headers: [],
    draggedIndex: -1,
    draggedColId: null,
    startX: 0,
    startY: 0,
    moved: false,
    lastDragAt: 0
};

function resetColumnInteractions() {
    columnResizeState.active = false;
    columnResizeState.pending = false;
    columnResizeState.moved = false;
    if (columnResizeState.header) {
        columnResizeState.header.classList.remove('resizing');
    }
    columnResizeState.header = null;
    columnResizeState.columnId = null;
    columnDragState.active = false;
    columnDragState.pending = false;
    columnDragState.clickHeader = null;
    columnDragState.clickStartX = 0;
    columnDragState.clickStartY = 0;
    columnDragState.headers.forEach(h => h.classList.remove('drag-over', 'dragging'));
    columnDragState.headers = [];
    columnDragState.draggedIndex = -1;
    columnDragState.draggedColId = null;
    columnDragState.moved = false;
    columnDragState.lastDragAt = 0;
    document.body.classList.remove('column-dragging');
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
}
let resizeHandlersBound = false;
let dragHandlersBound = false;

// Helper function to get column ID from header index
function getColumnIdFromIndex(index) {
    const visibleColumns = getVisibleColumns();
    if (index < 0 || index >= visibleColumns.length) return null;
    return visibleColumns[index];
}

// Initialize column resizing
function initColumnResize() {
    const table = document.querySelector('table');
    if (!table) return;
    
    const headers = table.querySelectorAll('thead th');
    
    // Restore saved column widths
    applyColumnWidths();
    
    headers.forEach((header, index) => {
        const handle = header.querySelector('.resize-border');
        if (!handle) return;
        handle.onmousedown = function(e) {
            if (Date.now() < sortClickLockUntil) return;
            e.preventDefault();
            e.stopPropagation();
            columnResizeState.pending = true;
            columnResizeState.active = false;
            columnResizeState.moved = false;
            columnResizeState.header = header;
            columnResizeState.columnId = getColumnIdFromIndex(index);
            columnResizeState.startX = e.pageX;
            columnResizeState.startWidth = header.offsetWidth;
        };
        header.onmousedown = null;
    });

    if (!resizeHandlersBound) {
        document.addEventListener('mousemove', handleColumnResizeMove);
        document.addEventListener('mouseup', handleColumnResizeUp);
        resizeHandlersBound = true;
    }
}

function updateColumnWidth(columnId, width) {
    const table = document.querySelector('table');
    if (!table || !columnId) return;
    const visibleColumns = getVisibleColumns();
    const colIndex = visibleColumns.indexOf(columnId);
    if (colIndex < 0) return;
    const headers = table.querySelectorAll('thead th');
    const header = headers[colIndex];
    if (header) {
        header.style.width = width + 'px';
        header.style.minWidth = width + 'px';
    }
    const allRows = table.querySelectorAll('tbody tr, tfoot tr');
    allRows.forEach(row => {
        const cell = row.children[colIndex];
        if (cell) {
            cell.style.width = width + 'px';
            cell.style.minWidth = width + 'px';
        }
    });
}

function handleColumnResizeMove(e) {
    if (!columnResizeState.pending || !columnResizeState.header || !columnResizeState.columnId) return;
    if (!columnResizeState.active) {
        if (Math.abs(e.pageX - columnResizeState.startX) < 6) return;
        columnResizeState.active = true;
        columnResizeState.moved = true;
        columnResizeState.header.classList.add('resizing');
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';
    }
    const diff = e.pageX - columnResizeState.startX;
    const newWidth = Math.max(40, columnResizeState.startWidth + diff);
    columnWidths[columnResizeState.columnId] = newWidth;
    updateColumnWidth(columnResizeState.columnId, newWidth);
}

function handleColumnResizeUp() {
    if (columnResizeState.active && columnResizeState.header) {
        columnResizeState.header.classList.remove('resizing');
    }
    if (columnResizeState.active && columnResizeState.columnId) {
        saveColumnWidths();
    }
    columnResizeState.active = false;
    columnResizeState.pending = false;
    columnResizeState.moved = false;
    columnResizeState.header = null;
    columnResizeState.columnId = null;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
}

function applyColumnWidths() {
    const table = document.querySelector('table');
    if (!table) return;
    
    const headers = table.querySelectorAll('thead th');
    const visibleColumns = getVisibleColumns();
    headers.forEach((header, colIndex) => {
        const columnId = visibleColumns[colIndex];
        if (columnId && columnWidths[columnId]) {
            const width = columnWidths[columnId] + 'px';
            header.style.width = width;
            header.style.minWidth = width;
            
            // Apply to all cells in this column
            const allRows = table.querySelectorAll('tbody tr, tfoot tr');
            allRows.forEach(row => {
                const cell = row.children[colIndex];
                if (cell) {
                    cell.style.width = width;
                    cell.style.minWidth = width;
                }
            });
        }
    });
}

// Initialize column drag-and-drop reordering
function initColumnReorder() {
    const thead = document.querySelector('thead tr.header-row');
    if (!thead) return;
    const getHeaders = () => Array.from(document.querySelectorAll('thead tr.header-row th'));
    const getTargetIndex = (clientX, headers) => {
        let idx = headers.length - 1;
        for (let i = 0; i < headers.length; i++) {
            const rect = headers[i].getBoundingClientRect();
            if (clientX < rect.left + rect.width / 2) {
                idx = i;
                break;
            }
        }
        return idx;
    };

    thead.onmousedown = function(e) {
        if (Date.now() < sortClickLockUntil) return;
        const header = e.target.closest('th');
        if (!header) return;
        if (e.button !== 0) return;
        if (e.target.closest('.resize-border')) return;
        const rect = header.getBoundingClientRect();
        if (rect.right - e.clientX <= 6) return;
        columnDragState.headers = getHeaders();
        columnDragState.draggedIndex = columnDragState.headers.indexOf(header);
        if (columnDragState.draggedIndex < 0) return;
        const visibleColumns = getVisibleColumns();
        columnDragState.draggedColId = visibleColumns[columnDragState.draggedIndex];
        columnDragState.startX = e.clientX;
        columnDragState.startY = e.clientY;
        columnDragState.moved = false;
        columnDragState.pending = true;
    };

    if (!dragHandlersBound) {
        document.addEventListener('mousemove', function(e) {
            if (!columnDragState.pending) return;
            if (Math.abs(e.clientX - columnDragState.startX) > 3 || Math.abs(e.clientY - columnDragState.startY) > 3) {
                columnDragState.moved = true;
                if (!columnDragState.active) {
                    columnDragState.active = true;
                    const header = columnDragState.headers[columnDragState.draggedIndex];
                    if (header) header.classList.add('dragging');
                    document.body.classList.add('column-dragging');
                    document.body.style.userSelect = 'none';
                }
            }
            columnDragState.headers.forEach(h => h.classList.remove('drag-over'));
            const targetIndex = getTargetIndex(e.clientX, columnDragState.headers);
            if (targetIndex >= 0 && targetIndex !== columnDragState.draggedIndex) {
                columnDragState.headers[targetIndex].classList.add('drag-over');
            }
        });

        document.addEventListener('mouseup', function(e) {
            if (!columnDragState.pending) return;
            const headersNow = getHeaders();
            const targetIndex = getTargetIndex(e.clientX, headersNow);
            if (columnDragState.active && columnDragState.draggedColId && targetIndex >= 0 && targetIndex !== columnDragState.draggedIndex) {
                const visibleColumns = getVisibleColumns();
                const targetColId = visibleColumns[targetIndex];
                const fromIndex = columnOrder.indexOf(columnDragState.draggedColId);
                if (fromIndex !== -1 && targetColId) {
                    columnOrder.splice(fromIndex, 1);
                    const insertIndex = columnOrder.indexOf(targetColId);
                    if (insertIndex === -1) {
                        columnOrder.push(columnDragState.draggedColId);
                    } else {
                        columnOrder.splice(insertIndex, 0, columnDragState.draggedColId);
                    }
                }
                saveColumnOrder();
                renderTable();
            }
            columnDragState.headers.forEach(h => h.classList.remove('drag-over', 'dragging'));
            columnDragState.headers = [];
            columnDragState.draggedIndex = -1;
            columnDragState.draggedColId = null;
            columnDragState.active = false;
            columnDragState.pending = false;
            document.body.classList.remove('column-dragging');
            document.body.style.userSelect = '';
            if (columnDragState.moved) {
                columnDragState.lastDragAt = Date.now();
            }
        });

        dragHandlersBound = true;
    }

    thead.onclick = null;
}

function applyAutoScale() {
    const baselineWidth = 1800;
    const baselineHeight = 1000;
    const scale = Math.min(window.innerWidth / baselineWidth, window.innerHeight / baselineHeight);
    const clamped = Math.max(0.6, Math.min(1.05, scale));
    document.documentElement.style.setProperty('--ui-scale', clamped.toFixed(3));
}

function applyResponsiveSidebar() {
    const sidebar = document.getElementById('sidebar');
    const toggle = document.getElementById('sidebar-toggle');
    if (!sidebar || !toggle) return;
    const shouldCollapse = window.innerWidth < 1250;
    sidebar.classList.toggle('collapsed', shouldCollapse);
    document.body.classList.toggle('sidebar-collapsed', shouldCollapse);
}

// Initialize theme + auto scale
const savedTheme = localStorage.getItem('theme') || 'light';
document.documentElement.setAttribute('data-theme', savedTheme);
document.body.setAttribute('data-theme', savedTheme);
document.documentElement.classList.toggle('dark', savedTheme === 'dark');
updateThemeButton();
applyAutoScale();
applyResponsiveSidebar();
window.addEventListener('resize', () => {
    applyAutoScale();
    applyResponsiveSidebar();
});

// Load column order
loadColumnOrder();

// Load hidden columns
loadHiddenColumns();

// Load column widths
loadColumnWidths();

loadSortState();
loadFilterState();
loadPendingEdits();
initGlobalHeaderSort();

// Load collapsed/expanded state for panels
function loadPanelStates() {
    const columnPanelExpanded = localStorage.getItem('column-visibility-panel-expanded') === 'true';
    const presetsPanelExpanded = localStorage.getItem('cost-presets-panel-expanded') === 'true';
    
    if (columnPanelExpanded) {
        document.getElementById('column-visibility-panel')?.classList.add('expanded');
        document.getElementById('column-visibility-panel-toggle').textContent = '▲';
    }
    if (presetsPanelExpanded) {
        document.getElementById('cost-presets-panel')?.classList.add('expanded');
        document.getElementById('cost-presets-panel-toggle').textContent = '▲';
    }
}

// Toggle collapsible panel - simplified and synchronous
window.toggleCollapsible = function(panelId) {
    const panel = document.getElementById(panelId);
    if (!panel) {
        console.error('Panel not found:', panelId);
        return;
    }
    
    const toggleId = panelId + '-toggle';
    const toggle = document.getElementById(toggleId);
    if (!toggle) {
        console.error('Toggle not found:', toggleId);
        return;
    }
    
    // Toggle the expanded class
    panel.classList.toggle('expanded');
    const isExpanded = panel.classList.contains('expanded');
    toggle.textContent = isExpanded ? '▲' : '▼';
    
    // Save state
    localStorage.setItem(panelId + '-expanded', isExpanded.toString());
};

// Also define without window prefix for easier access
function toggleCollapsible(panelId) {
    window.toggleCollapsible(panelId);
}

// Set up event listeners for collapsible panels (more reliable than onclick)
function setupCollapsiblePanels() {
    document.querySelectorAll('.collapsible-header').forEach(function(header) {
        header.addEventListener('click', function() {
            const panelId = this.getAttribute('data-panel-id');
            if (panelId) {
                window.toggleCollapsible(panelId);
            }
        });
    });
}

// Set up listeners when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function() {
        setupCollapsiblePanels();
    });
} else {
    setupCollapsiblePanels();
}

loadPanelStates();

// Initialize image size slider
initImageSizeSlider();
initFontSizeSlider();
setTableBold(localStorage.getItem('tableBold') === 'true');
initSidebarState();
initDeleteConfirmModal();
initFileMenu();
initAiControls();
initSearch();

// Export CSV function
async function exportCSV() {
    if (!currentShowId) {
        alert('Please select a show first');
        return;
    }
    
    try {
        const response = await fetch(`/api/items/export?show_id=${currentShowId}`);
        if (!response.ok) {
            const error = await response.json();
            alert('Error exporting CSV: ' + (error.error || 'Unknown error'));
            return;
        }
        
        // Get filename from Content-Disposition header or use default
        const contentDisposition = response.headers.get('Content-Disposition');
        let filename = 'export.csv';
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename="(.+)"/);
            if (filenameMatch) {
                filename = filenameMatch[1];
            }
        }
        
        // Download the file
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        
        alert('CSV exported successfully!');
    } catch (error) {
        console.error('Error exporting CSV:', error);
        alert('Error exporting CSV: ' + error.message);
    }
}

async function exportShow() {
    if (!currentShowId) {
        alert('Please select a show first');
        return;
    }

    try {
        const response = await fetch(`/api/shows/${currentShowId}/export`);
        if (!response.ok) {
            const error = await response.json();
            alert('Error exporting show: ' + (error.error || 'Unknown error'));
            return;
        }
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        const contentDisposition = response.headers.get('Content-Disposition') || '';
        const match = contentDisposition.match(/filename="([^"]+)"/);
        const filename = match ? match[1] : 'show_export.zip';
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
    } catch (error) {
        console.error('Error exporting show:', error);
        alert('Error exporting show: ' + error.message);
    }
}

function generateSummary() {
    if (!currentShowId) {
        alert('Please select a show first');
        return;
    }
    const commissionInput = document.getElementById('commission-rate');
    const commissionRate = commissionInput ? commissionInput.value : '8';
    const url = `/shows/${currentShowId}/summary?commission_rate=${encodeURIComponent(commissionRate)}`;
    window.open(url, '_blank');
}

// Import CSV function
async function importCSV(event) {
    const file = event.target.files[0];
    if (!file) {
        return;
    }
    
    if (!currentShowId) {
        alert('Please select a show first');
        event.target.value = ''; // Reset file input
        return;
    }
    
    if (!file.name.endsWith('.csv')) {
        alert('Please select a CSV file');
        event.target.value = ''; // Reset file input
        return;
    }
    
    if (!confirm(`Import CSV file "${file.name}" into the current show? This will merge/update existing items.`)) {
        event.target.value = ''; // Reset file input
        return;
    }
    
    try {
        const formData = new FormData();
        formData.append('file', file);
        formData.append('show_id', currentShowId);
        
        const response = await fetch('/api/items/import', {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (!response.ok) {
            alert('Error importing CSV: ' + (result.error || 'Unknown error'));
            event.target.value = ''; // Reset file input
            return;
        }
        
        alert(`Successfully imported ${result.imported} items. ${result.updated > 0 ? result.updated + ' items updated in database.' : ''}`);
        
        // Reload data to show imported items
        await loadData();
        
        event.target.value = ''; // Reset file input
    } catch (error) {
        console.error('Error importing CSV:', error);
        alert('Error importing CSV: ' + error.message);
        event.target.value = ''; // Reset file input
    }
}

async function importShow(event) {
    const file = event.target.files[0];
    if (!file) {
        return;
    }

    if (!file.name.endsWith('.zip')) {
        alert('Please select a .zip file');
        event.target.value = '';
        return;
    }

    if (!confirm(`Import show from "${file.name}"? This will add a new show.`)) {
        event.target.value = '';
        return;
    }

    try {
        const formData = new FormData();
        formData.append('file', file);

        const response = await fetch('/api/shows/import', {
            method: 'POST',
            body: formData
        });

        const result = await response.json();

        if (!response.ok) {
            alert('Error importing show: ' + (result.error || 'Unknown error'));
            event.target.value = '';
            return;
        }

        await loadShows();
        currentShowId = result.id;
        const showSelect = document.getElementById('show-select');
        if (showSelect) {
            showSelect.value = result.id;
        }
        await loadData();
        alert(`Show imported: ${result.name}`);
        event.target.value = '';
    } catch (error) {
        console.error('Error importing show:', error);
        alert('Error importing show: ' + error.message);
        event.target.value = '';
    }
}

// Initialize column resize
initColumnResize();

// Initialize spreadsheet selection
initSpreadsheetSelection();

const tableBody = document.getElementById('items-table-body');
if (tableBody) {
    tableBody.addEventListener('click', (event) => {
        const cell = event.target.closest('td');
        const index = getRowIndexFromCell(cell);
        if (index !== null) {
            lastActiveRowIndex = index;
        }
    });
}

const tableCard = document.querySelector('.table-card');
const interactionHandler = () => {
    lastUserInteractionAt = Date.now();
};
if (tableCard) {
    tableCard.addEventListener('mousedown', interactionHandler);
    tableCard.addEventListener('keydown', interactionHandler);
    tableCard.addEventListener('wheel', interactionHandler, { passive: true });
    tableCard.addEventListener('touchstart', interactionHandler, { passive: true });
}

// Fetch user info (role + column permissions) before loading data
async function loadUserInfo() {
    try {
        const resp = await fetch('/api/user-info');
        if (resp.ok) {
            const info = await resp.json();
            userRole = info.role || 'owner';
            userVisibleColumns = info.visible_columns; // null for owner, array for employee
            // If employee, force-hide columns they can't see
            if (userVisibleColumns) {
                hiddenColumns = columnConfig
                    .map(c => c.id)
                    .filter(id => !userVisibleColumns.includes(id));
            }
            // Hide owner-only UI elements for employees
            if (userRole !== 'owner') {
                document.querySelectorAll('.owner-only').forEach(el => {
                    el.style.display = 'none';
                });
            }
        }
    } catch (e) {
        console.warn('Could not load user info:', e);
    }
}

// Load user info, then shows, then data
loadUserInfo().then(() => loadShows()).then(async () => {
    await loadData({ force: true });
    // Ensure tfoot exists after data loads (columnOrder is now defined)
    updateTfootTotals();
    // Update column visibility panel
    updateColumnVisibilityMenu();
    // Check recording status on page load
    checkRecordingStatus();
    // Poll recording status every 3 seconds
    recordingStatusInterval = setInterval(checkRecordingStatus, 3000);
    // Restore scroll after initial render
    requestAnimationFrame(() => {
        restoreScrollPosition();
    });
});
