from flask import Flask, render_template_string, request, redirect, url_for
from urllib.parse import urlencode
import subprocess
import threading
import os
import pandas as pd

LOCAL_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(LOCAL_DIR, 'Threaded_cpu_usage.csv')

REFRESH_STATUS_FILE = os.path.join(LOCAL_DIR, 'refresh_status.txt')

# Columns that support threshold filters (show rows where value > X%)
THRESHOLD_COLUMNS = {'%Busy_Socket0', '%Busy_Socket1', '%Free_Socket0', '%Free_Socket1'}
THRESHOLD_OPTIONS = ['>50%', '>75%', '> 85%', '> 90%', '> 95%']

app = Flask(__name__)
@app.route('/status')
def status():
    refresh_status, last_refresh, refresh_error = get_refresh_status()
    return {'status': refresh_status, 'last_refresh': last_refresh, 'error': refresh_error}
def set_refresh_status(status, dt=None, error=None):
    with open(REFRESH_STATUS_FILE, 'w') as f:
        line = f"{status}|{dt if dt else ''}"
        if error:
            line += f"|{error}"
        f.write(line)

def get_refresh_status():
    if not os.path.exists(REFRESH_STATUS_FILE):
        return 'idle', '', ''
    with open(REFRESH_STATUS_FILE) as f:
        parts = f.read().split('|')
        status = parts[0] if parts else 'idle'
        dt = parts[1] if len(parts) > 1 else ''
        error = parts[2] if len(parts) > 2 else ''
        return status, dt, error

def run_refresh():
    import datetime
    set_refresh_status('running')
    env = os.environ.copy()
    env['REFRESH_STATUS_FILE'] = REFRESH_STATUS_FILE
    try:
        # Use conda run to execute the script in the 'check_cpu_usage' environment
        # Pass REFRESH_STATUS_FILE so the script can update status when it finishes
        # (handles PM2 restarts where the Flask thread may be killed before subprocess returns)
        result = subprocess.run(
            [
                'conda', 'run', '-n', 'check_cpu_usage', 'python',
                os.path.join(LOCAL_DIR, 'check_cpu_usage.py')
            ],
            capture_output=True,
            text=True,
            cwd=LOCAL_DIR,
            env=env,
        )
        dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if result.returncode == 0:
            set_refresh_status('idle', dt)
        else:
            set_refresh_status('error', dt, error=result.stderr or result.stdout)
    except Exception as e:
        dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        set_refresh_status('error', dt, error=str(e))

@app.route('/refresh', methods=['POST'])
def refresh():
    status, _, _ = get_refresh_status()
    if status != 'running':
        threading.Thread(target=run_refresh, daemon=True).start()
    # Preserve filters and sort when redirecting (skip if CSV doesn't exist yet)
    try:
        columns = pd.read_csv(CSV_FILE).columns.tolist()
    except (FileNotFoundError, pd.errors.EmptyDataError):
        columns = []
    params = []
    for col in columns:
        for v in request.form.getlist(f'filter_{col}'):
            params.append((f'filter_{col}', v))
    params.append(('sort_col', request.form.get('sort_col', '') or ''))
    params.append(('sort_dir', request.form.get('sort_dir', 'asc') or 'asc'))
    query = urlencode(params)
    return redirect(url_for('index') + ('?' + query))

def get_filtered_data(filters=None):
    df = pd.read_csv(CSV_FILE)
    if filters:
        for col, vals in filters.items():
            if not vals:
                continue
            # Separate threshold filters ("> 5%", etc.) from exact-match filters
            threshold_vals = [v for v in vals if v in THRESHOLD_OPTIONS]
            exact_vals = [v for v in vals if v not in THRESHOLD_OPTIONS]
            if col in THRESHOLD_COLUMNS and threshold_vals:
                # Parse threshold (e.g. "> 90%" -> 90) and keep rows where value > max(thresholds)
                thresholds = []
                for v in threshold_vals:
                    try:
                        n = float(v.replace('>', '').replace('%', '').strip())
                        thresholds.append(n)
                    except ValueError:
                        pass
                if thresholds:
                    thresh = max(thresholds)
                    numeric_col = pd.to_numeric(df[col], errors='coerce')
                    thresh_mask = numeric_col > thresh
                    if exact_vals:
                        exact_mask = df[col].astype(str).isin(exact_vals)
                        mask = thresh_mask | exact_mask
                    else:
                        mask = thresh_mask
                    df = df[mask]
            elif exact_vals:
                df = df[df[col].astype(str).isin(exact_vals)]
    return df

@app.route('/', methods=['GET', 'POST'])
def index():
    # If CSV doesn't exist, trigger refresh and show waiting page
    if not os.path.exists(CSV_FILE):
        status, _, _ = get_refresh_status()
        if status != 'running':
            threading.Thread(target=run_refresh, daemon=True).start()
        return render_template_string('''
        <html>
        <head><title>Threaded CPU Usage</title>
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.6.2/dist/css/bootstrap.min.css">
        </head>
        <body class="container mt-5">
            <h1>Threaded CPU Usage</h1>
            <p class="lead">No data file found. Running refresh to pull in data...</p>
            <p>This page will reload automatically when the refresh completes.</p>
            <div class="mt-3 text-muted" style="font-size: 12px;">
                GUI Info: {gui_script} — {gui_path}<br>
                API Info: {api_script} — {api_path}
            </div>
            <script>
            function poll() {
                fetch('/status')
                    .then(r => r.json())
                    .then(d => {
                        if (d.status !== 'running') window.location.reload();
                        else setTimeout(poll, 2000);
                    })
                    .catch(function() { setTimeout(poll, 2000); });
            }
            setTimeout(poll, 2000);
            </script>
        </body>
        </html>
        '''.format(
            gui_script=os.path.basename(__file__),
            gui_path=os.path.abspath(__file__),
            api_script='check_cpu_usage.py',
            api_path=os.path.abspath(os.path.join(LOCAL_DIR, 'check_cpu_usage.py'))))
    df = pd.read_csv(CSV_FILE)
    refresh_status, last_refresh, refresh_error = get_refresh_status()
    columns = df.columns.tolist()
    # Build filter options for each column
    filter_options = {}
    for col in columns:
        opts = [str(val) for val in sorted(df[col].dropna().unique())]
        if col in THRESHOLD_COLUMNS:
            opts = THRESHOLD_OPTIONS + opts
        filter_options[col] = opts
    filters = {}
    clear = request.form.get('clear')
    for col in columns:
        filters[col] = [] if clear else (request.form.getlist(f'filter_{col}') or request.args.getlist(f'filter_{col}'))
    # Sorting (from form or query string)
    sort_col = request.form.get('sort_col') or request.args.get('sort_col')
    sort_dir = request.form.get('sort_dir') or request.args.get('sort_dir') or 'asc'
    # If all filters are empty, show all data
    active_filters = {k: v for k, v in filters.items() if v}
    filtered_df = get_filtered_data(active_filters) if active_filters else df
    if sort_col in columns:
        filtered_df = filtered_df.sort_values(by=sort_col, ascending=(sort_dir=='asc'))
    # Custom table rendering with filter dropdowns in header row
    return render_template_string('''
    <html>
    <head>
        <title>Threaded CPU Usage</title>
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.6.2/dist/css/bootstrap.min.css">
        <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
        <style>
            html, body {
                height: 100%;
                margin: 0;
                padding: 0;
            }
            body, table, th, td, .form-control, .btn, .filter-header {
                font-size: 13px !important;
            }
            h1 { font-size: 20px; }
            .filter-dropdown label { font-size: 12px; }
            .container {
                width: 98vw;
                max-width: 100vw;
                margin: 0 auto;
                padding: 0 1vw;
            }
            .table-responsive {
                width: 100%;
                overflow-x: visible;
            }
            table.table {
                width: 100% !important;
                border-collapse: collapse !important;
                table-layout: auto;
            }
            td, th {
                white-space: nowrap;
                overflow-x: auto;
                min-width: 120px;
                max-width: 600px;
                border: 1px solid #888 !important;
            }
            table.table, th, td {
                border: 1px solid #888 !important;
            }
            .refresh-btn {
                position: absolute;
                top: 20px;
                right: 30px;
                z-index: 3000;
            }
            .refresh-status {
                position: absolute;
                top: 55px;
                right: 30px;
                font-size: 12px;
                color: #555;
                z-index: 3000;
            }
            .refresh-error {
                position: absolute;
                top: 75px;
                right: 30px;
                font-size: 12px;
                color: red;
                z-index: 3000;
                max-width: 400px;
                word-break: break-all;
            }
            .filter-icon {
                cursor: pointer;
                margin-left: 6px;
                padding: 2px 6px;
                border-radius: 3px;
                font-size: 11px;
                background: #e9ecef;
                display: inline-block;
                user-select: none;
            }
            .filter-icon:hover {
                background: #007bff;
                color: white;
            }
            .filter-icon.filter-active {
                background: #28a745;
                color: white;
            }
            .filter-icon.filter-active:hover {
                background: #218838;
                color: white;
            }
            #filterPopupContainer {
                position: fixed;
                z-index: 5000;
                background: white;
                border: 1px solid #ccc;
                border-radius: 4px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.15);
                padding: 10px;
                max-height: 300px;
                overflow-y: auto;
            }
            .filter-popup-checkboxes {
                max-height: 220px;
                overflow-y: auto;
            }
            .filter-popup-checkboxes label {
                display: block;
                margin: 4px 0;
                cursor: pointer;
                font-weight: normal;
            }
            .dt-entries-search {
                display: flex;
                align-items: center;
                gap: 15px;
                flex-wrap: wrap;
            }
            .dt-entries-search .dataTables_length,
            .dt-entries-search .dataTables_filter {
                margin: 0;
            }
            .clear-filters-btn {
                margin-left: 10px;
            }
        </style>
        <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
        <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
        <script>
        $(function() {
            var table = $('#cpuTable').DataTable({
                paging: true,
                searching: true,
                ordering: true,
                order: [],
                autoWidth: false,
                scrollX: true,
                pageLength: 25,
                stateSave: true,
                stateSaveCallback: function(settings, data) {
                    var scrollBody = $('#cpuTable').closest('.dataTables_scrollBody');
                    if (scrollBody.length) {
                        data.scrollLeft = scrollBody.scrollLeft();
                    }
                    localStorage.setItem('DataTables_cpuTable', JSON.stringify(data));
                },
                stateLoadCallback: function(settings) {
                    var saved = localStorage.getItem('DataTables_cpuTable');
                    return saved ? JSON.parse(saved) : null;
                },
                initComplete: function(settings) {
                    var scrollBody = $('#cpuTable').closest('.dataTables_scrollBody');
                    if (scrollBody.length) {
                        var scrollLeft = sessionStorage.getItem('cpuTable_scrollLeft');
                        if (scrollLeft === null) {
                            var saved = localStorage.getItem('DataTables_cpuTable');
                            if (saved) {
                                try {
                                    var data = JSON.parse(saved);
                                    scrollLeft = data.scrollLeft;
                                } catch (e) {}
                            }
                        }
                        if (scrollLeft !== null && scrollLeft !== undefined) {
                            var pos = parseInt(scrollLeft, 10);
                            setTimeout(function() { scrollBody.scrollLeft(pos); }, 0);
                        }
                        sessionStorage.removeItem('cpuTable_scrollLeft');
                    }
                },
                dom: '<"row"<"col-sm-12"<"dt-entries-search"lf>>>rtip'
            });
            var columns = {{ columns|tojson|safe }};
            var filterOptionsByCol = {};
            {% for col in columns %}
            filterOptionsByCol[{{loop.index0}}] = {{ filter_options[col]|tojson|safe }};
            {% endfor %}
            var currentFiltersByCol = {};
            {% for col in columns %}
            currentFiltersByCol[{{loop.index0}}] = {{ filters[col]|tojson|safe }};
            {% endfor %}
            // Add Clear all filters button (always visible; red when filters are active)
            var hasFilters = Object.values(currentFiltersByCol).some(function(arr) { return arr.length > 0; });
            var clearBtn = $('<button type="submit" name="clear" value="1" class="btn btn-sm clear-filters-btn">Clear all filters</button>');
            clearBtn.addClass(hasFilters ? 'btn-danger' : 'btn-outline-secondary');
            $('.dt-entries-search').append(clearBtn);
            // Use capture phase so we run BEFORE DataTables - prevents sort when clicking filter icon
            document.addEventListener('click', function(event) {
                var icon = event.target.closest && event.target.closest('.filter-icon');
                if (!icon) return;
                event.preventDefault();
                event.stopPropagation();
                event.stopImmediatePropagation();
                var colIdx = parseInt(icon.getAttribute('data-col'), 10);
                var colName = columns[colIdx];
                var values = filterOptionsByCol[colIdx] || [];
                var currentFilters = currentFiltersByCol[colIdx] || [];
                showFilterDropdown(colIdx, colName, values, currentFilters, icon);
            }, true);
            window.applyFilterFromPopup = function(colIdx) {
                var colName = columns[colIdx];
                var checked = [];
                $('#filterPopupContainer .filter-checkbox:checked').each(function() {
                    checked.push($(this).val());
                });
                // Update hidden inputs for this column
                $('#filterInputs input').filter(function() { return $(this).attr('name') === 'filter_' + colName; }).remove();
                checked.forEach(function(val) {
                    var inp = $('<input type="hidden">').attr('name', 'filter_' + colName).val(val);
                    $('#filterInputs').append(inp);
                });
                $('#filterPopupContainer').hide();
                // Save scroll position before submit so it can be restored on reload
                var scrollBody = $('#cpuTable').closest('.dataTables_scrollBody');
                if (scrollBody.length) {
                    sessionStorage.setItem('cpuTable_scrollLeft', scrollBody.scrollLeft());
                }
                $('#filterForm').submit();
            };
            $('#filterForm').on('submit', function() {
                var scrollBody = $('#cpuTable').closest('.dataTables_scrollBody');
                if (scrollBody.length) {
                    sessionStorage.setItem('cpuTable_scrollLeft', scrollBody.scrollLeft());
                }
            });
            window.showFilterDropdown = function(colIdx, colName, values, currentFilters, anchorEl) {
                $('#filterPopupContainer').hide();
                var el = anchorEl ? anchorEl : document.querySelector('#header_' + colIdx + ' .filter-icon, #header_' + colIdx);
                var rect = el ? el.getBoundingClientRect() : { left: 0, bottom: 0 };
                var checkboxesHtml = '';
                values.forEach(function(val) {
                    var checked = currentFilters.indexOf(val) >= 0 ? ' checked' : '';
                    var escapedVal = ('' + val).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
                    checkboxesHtml += '<label><input type="checkbox" class="filter-checkbox" value="' + escapedVal + '"' + checked + '> ' + escapedVal + '</label>';
                });
                var clearBtnHtml = currentFilters.length > 0
                    ? "<button type='button' class='btn btn-sm btn-outline-danger mr-1' onclick='clearColumnFilter(" + colIdx + ")'>Clear filter</button> "
                    : "";
                var popupHtml = "<div style='font-weight:600;margin-bottom:6px;'>Filter " + colName + "</div>" +
                    "<div class='filter-popup-checkboxes'>" + checkboxesHtml + "</div>" +
                    "<div class='mt-2'>" + clearBtnHtml +
                    "<button type='button' class='btn btn-sm btn-primary' onclick='applyFilterFromPopup(" + colIdx + ")'>Apply</button> " +
                    "<button type='button' class='btn btn-sm btn-secondary' onclick='hideFilterDropdown()'>Close</button></div>";
                $('#filterPopupContainer').html(popupHtml);
                var minW = Math.max(rect.width || 0, 180);
                $('#filterPopupContainer').css({
                    display: 'block',
                    left: rect.left + 'px',
                    top: rect.bottom + 'px',
                    minWidth: minW + 'px'
                });
            };
            window.hideFilterDropdown = function() {
                $('#filterPopupContainer').hide();
            };
            window.clearColumnFilter = function(colIdx) {
                var colName = columns[colIdx];
                $('#filterInputs input').filter(function() { return $(this).attr('name') === 'filter_' + colName; }).remove();
                $('#filterPopupContainer').hide();
                var scrollBody = $('#cpuTable').closest('.dataTables_scrollBody');
                if (scrollBody.length) {
                    sessionStorage.setItem('cpuTable_scrollLeft', scrollBody.scrollLeft());
                }
                $('#filterForm').submit();
            };
            $(document).on('mousedown', function(event) {
                if (!$(event.target).closest('#filterPopupContainer, .filter-icon').length) {
                    $('#filterPopupContainer').hide();
                }
            });
        });
        // Auto-refresh when status changes from running to idle or error
        var lastStatus = '{{refresh_status}}';
        var pollInterval = 2000;
        function pollRefreshStatus() {
            fetch('/status')
                .then(response => response.json())
                .then(data => {
                    var newStatus = data.status;
                    if (lastStatus === 'running' && newStatus !== 'running') {
                        window.location.reload();
                    } else {
                        lastStatus = newStatus;
                        setTimeout(pollRefreshStatus, pollInterval);
                    }
                })
                .catch(function(err) {
                    // Connection reset, network error, etc. - retry after delay
                    setTimeout(pollRefreshStatus, pollInterval);
                });
        }
        if (lastStatus === 'running') {
            setTimeout(pollRefreshStatus, pollInterval);
        }
        </script>
    </head>
    <body>
        <button type="submit" form="filterForm" formaction="/refresh" class="btn refresh-btn" style="background-color:white;border:1px solid #ccc;{% if refresh_status == 'running' %}color:red;{% endif %}">
            {% if refresh_status == 'running' %}Running...{% else %}Refresh Data{% endif %}
        </button>
        <div class="refresh-status">
            {% if last_refresh %}Last refreshed: {{last_refresh}}{% endif %}
        </div>
        {% if refresh_status == 'error' and refresh_error %}
        <div class="refresh-error">
            <b>Error:</b> {{refresh_error}}
        </div>
        {% endif %}
        <div id="filterPopupContainer" style="display:none;"></div>
        <div class="container">
            <h1>Threaded CPU Usage</h1>
            <form method="post" action="/" id="filterForm">
            <div id="filterInputs">
                {% for col in columns %}
                {% for v in filters[col] %}
                <input type="hidden" name="filter_{{col}}" value="{{v}}">
                {% endfor %}
                {% endfor %}
                <input type="hidden" name="sort_col" value="{{ sort_col or '' }}">
                <input type="hidden" name="sort_dir" value="{{ sort_dir or 'asc' }}">
            </div>
            <div class="table-responsive">
            <table class="table table-striped" id="cpuTable">
                <thead>
                    <tr>
                    {% for col in columns %}
                        <th id="header_{{loop.index0}}">{{col}} <span class="filter-icon{% if filters[col] %} filter-active{% endif %}" data-col="{{loop.index0}}" title="Filter column">Filter</span></th>
                    {% endfor %}
                    </tr>
                </thead>
                <tbody>
                    {% if filtered_df.shape[0] == 0 %}
                    <tr>
                        {% for col in columns %}
                        <td class="text-center">{% if loop.first %}No data to display{% endif %}</td>
                        {% endfor %}
                    </tr>
                    {% else %}
                    {% for row in filtered_df.values.tolist() %}
                    <tr>
                        {% for cell in row %}
                            <td>{{cell}}</td>
                        {% endfor %}
                    </tr>
                    {% endfor %}
                    {% endif %}
                </tbody>
            </table>
            </div>
            </form>
            <div class="mt-3 text-muted" style="font-size: 12px;">
                GUI Info: {{ gui_script }} — {{ gui_path }}<br>
                API Info: {{ api_script }} — {{ api_path }}
            </div>
        </div>
    </body>
    </html>
    ''', columns=columns, filter_options=filter_options, filters=filters, filtered_df=filtered_df, sort_col=sort_col, sort_dir=sort_dir, refresh_status=refresh_status, last_refresh=last_refresh, refresh_error=refresh_error,
        gui_script=os.path.basename(__file__),
        gui_path=os.path.abspath(__file__),
        api_script='check_cpu_usage.py',
        api_path=os.path.abspath(os.path.join(LOCAL_DIR, 'check_cpu_usage.py')))

if __name__ == '__main__':
    # Use debug=False with PM2 - debug mode's reloader can cause ERR_CONNECTION_RESET
    use_debug = os.environ.get('FLASK_DEBUG', '0') == '1'
    app.run(debug=use_debug, host='0.0.0.0', port=8456, threaded=True)
