

from flask import Flask, render_template_string, request, redirect, url_for
import subprocess
import threading
import os
import pandas as pd

CSV_FILE = 'Threaded_cpu_usage.csv'
REFRESH_STATUS_FILE = 'refresh_status.txt'

app = Flask(__name__)
@app.route('/status')


def status():
    refresh_status, last_refresh, refresh_error = get_refresh_status()
    return {'status': refresh_status, 'last_refresh': last_refresh, 'error': refresh_error}

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
    pixi_python = os.path.join(os.path.dirname(__file__), '.pixi', 'envs', 'default', 'bin', 'python')
    set_refresh_status('running')
    try:
        result = subprocess.run([pixi_python, 'check_cpu_usage_threading_version.py'], cwd=os.path.dirname(__file__), capture_output=True, text=True)
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
    return redirect(url_for('index'))

def get_filtered_data(filters=None):
    df = pd.read_csv(CSV_FILE)
    if filters:
        for col, vals in filters.items():
            if vals:
                df = df[df[col].isin(vals)]
    return df

@app.route('/', methods=['GET', 'POST'])
def index():
    df = pd.read_csv(CSV_FILE)
    refresh_status, last_refresh, refresh_error = get_refresh_status()
    columns = df.columns.tolist()
    # Build filter options for each column
    filter_options = {col: sorted(df[col].dropna().unique()) for col in columns}
    filters = {}
    clear = request.form.get('clear')
    for col in columns:
        filters[col] = [] if clear else request.form.getlist(f'filter_{col}')
    # Sorting
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
        <style>
            body, table, th, td, .form-control, .btn, .filter-header {
                font-size: 13px !important;
            }
            h1 { font-size: 20px; }
            .filter-dropdown label { font-size: 12px; }
            td, th {
                white-space: nowrap;
                overflow-x: auto;
                min-width: 120px;
                max-width: 600px;
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
        </style>
        <script>
        function toggleDropdown(col) {
            var el = document.getElementById('dropdown_' + col);
            var header = document.getElementById('header_' + col);
            if (el.style.display === 'none' || el.style.display === '') {
                // Get header position
                var rect = header.getBoundingClientRect();
                el.style.display = 'block';
                el.style.position = 'fixed';
                el.style.top = (rect.bottom + window.scrollY) + 'px';
                el.style.left = (rect.left + window.scrollX) + 'px';
            } else {
                el.style.display = 'none';
            }
        }
        document.addEventListener('mousedown', function(event) {
            var dropdowns = document.querySelectorAll('.filter-dropdown');
            var isHeader = event.target.classList.contains('filter-header');
            var isDropdown = event.target.closest('.filter-dropdown');
            var isApplyBtn = event.target.classList.contains('btn-primary');
            if (!isHeader && !isDropdown && !isApplyBtn) {
                dropdowns.forEach(function(dd) { dd.style.display = 'none'; });
            }
        });
        function closeDropdown(col) {
            var el = document.getElementById('dropdown_' + col);
            el.style.display = 'none';
        }
        function sortColumn(col) {
            var form = document.getElementById('mainForm');
            var sortColInput = document.getElementById('sort_col_input');
            var sortDirInput = document.getElementById('sort_dir_input');
            var currentCol = sortColInput.value;
            var currentDir = sortDirInput.value || 'asc';
            var newDir = (currentCol === col && currentDir === 'asc') ? 'desc' : 'asc';
            sortColInput.value = col;
            sortDirInput.value = newDir;
            form.submit();
        }

        // Auto-refresh when status changes from running to idle or error
        var lastStatus = '{{refresh_status}}';
        function pollRefreshStatus() {
            fetch('/status')
                .then(response => response.json())
                .then(data => {
                    var newStatus = data.status;
                    if (lastStatus === 'running' && newStatus !== 'running') {
                        window.location.reload();
                    } else {
                        lastStatus = newStatus;
                        setTimeout(pollRefreshStatus, 2000);
                    }
                });
        }
        if (lastStatus === 'running') {
            setTimeout(pollRefreshStatus, 2000);
        }
        </script>
    </head>
    <body>
        <form method="post" action="/refresh" style="display:inline;">
            <button type="submit" class="btn refresh-btn" style="background-color:white;border:1px solid #ccc;{% if refresh_status == 'running' %}color:red;{% endif %}">
                {% if refresh_status == 'running' %}Running...{% else %}Refresh Data{% endif %}
            </button>
        </form>
        <div class="refresh-status">
            {% if last_refresh %}Last refreshed: {{last_refresh}}{% endif %}
        </div>
        {% if refresh_status == 'error' and refresh_error %}
        <div class="refresh-error">
            <b>Error:</b> {{refresh_error}}
        </div>
        {% endif %}
        <div class="container">
            <h1>Threaded CPU Usage</h1>
            <form method="post" id="mainForm">
                <input type="hidden" name="sort_col" id="sort_col_input" value="{{sort_col or ''}}">
                <input type="hidden" name="sort_dir" id="sort_dir_input" value="{{sort_dir or 'asc'}}">
                <div class="mb-2">
                    <button type="submit" name="clear" value="1" class="btn btn-secondary">Clear Filters</button>
                </div>
                <div class="table-responsive">
                <table class="table table-striped">
                    <thead>
                        <tr>
                        {% for col in columns %}
                            <th id="header_{{col}}" style="position:relative; min-width:120px; max-width:600px;">
                                <div style="display:flex;align-items:center;justify-content:space-between;">
                                    <span class="filter-header" style="cursor:pointer;" onclick="toggleDropdown('{{col}}')">{{col}}</span>
                                    <span style="cursor:pointer;" onclick="sortColumn('{{col}}')">&#8597;</span>
                                </div>
                                <div id="dropdown_{{col}}" class="filter-dropdown" style="display:none;position:fixed;z-index:2000;background:#fff;border:1px solid #ccc;padding:8px;min-width:150px;max-width:600px;width:auto;box-shadow:0 2px 8px rgba(0,0,0,0.15);">
                                    <label style="font-weight:normal;">Filter {{col}}:</label><br>
                                    <select name="filter_{{col}}" multiple class="form-control" style="width:100%;max-width:580px;{% if filter_options[col]|length > 20 %}max-height:350px;overflow-y:auto;{% endif %}">
                                        {% for val in filter_options[col] %}
                                            <option value="{{val}}" {% if val in filters[col] %}selected{% endif %} style="width:auto;max-width:580px;">{{val}}</option>
                                        {% endfor %}
                                    </select>
                                    <button type="submit" class="btn btn-sm btn-primary mt-2" onclick="closeDropdown('{{col}}')">Apply</button>
                                </div>
                            </th>
                        {% endfor %}
                        </tr>
                    </thead>
                    <tbody>
                        {% if filtered_df.shape[0] == 0 %}
                        <tr><td colspan="{{columns|length}}" class="text-center">No data to display</td></tr>
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
        </div>
    </body>
    </html>
    ''', columns=columns, filter_options=filter_options, filters=filters, filtered_df=filtered_df, sort_col=sort_col, sort_dir=sort_dir, refresh_status=refresh_status, last_refresh=last_refresh, refresh_error=refresh_error)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5050)
