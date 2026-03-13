import paramiko
import csv
import os
import ast
import json
import subprocess
from typing import List, Dict, Optional
import time
import logging
from collections import defaultdict
from pgserver import PostgresQueryRunner

SSH_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config')
OUTPUT_CSV = 'Threaded_cpu_usage.csv'  # Replace with actual URL
LOG_FILE = os.path.join(os.path.dirname(__file__), 'check_cpu_usage.log')


def create_logger(
    name: str = __name__,
    log_file: Optional[str] = None,
    level: int = logging.INFO,
) -> logging.Logger:
    """Create a logger that writes to both a file and stdout."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    # Avoid adding handlers multiple times if logger already configured
    if logger.handlers:
        return logger
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    # File handler
    file_path = log_file or LOG_FILE
    fh = logging.FileHandler(file_path, encoding='utf-8')
    fh.setLevel(level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    # Stdout handler
    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger

def parse_ssh_config(config_path: str) -> List[Dict[str, str]]:
    servers = []
    with open(config_path, 'r') as f:
        lines = f.readlines()
    current = {}
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if line.lower().startswith('server '):
            if current:
                servers.append(current)
            current = {'server': line.split()[1]}
        elif line.lower().startswith('servername '):
            current['servername'] = line.split()[1]
        elif line.lower().startswith('user '):
            current['user'] = line.split()[1]
    if current:
        servers.append(current)
    # Remove wildcard servers
    servers = [h for h in servers if '*' not in h['server']]
    return servers

def parse_cpu_list(cpu_list_str):
    # Parses CPU list like '2,3,5-7' into [2,3,5,6,7]
    cpus = set()
    if not cpu_list_str or cpu_list_str.lower() == 'none':
        return []
    for part in cpu_list_str.split(','):
        if '-' in part:
            start, end = part.split('-')
            cpus.update(range(int(start), int(end)+1))
        else:
            cpus.add(int(part))
    return sorted(cpus)

def get_cpu_idle_status(
    servername: str, ssh, isolated, logger: Optional[logging.Logger] = None
) -> List[Dict[str, str]]:
    results = []
    try:
        '''
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=servername, username=user)

        # Get isolated CPUs
       
        iso_cmd = "cat /sys/devices/system/cpu/isolated 2>/dev/null || grep -o 'isolcpus=[^ ]*' /proc/cmdline | cut -d= -f2"
        stdin, stdout, stderr = ssh.exec_command(iso_cmd)
        isolated = stdout.read().decode().strip() or ''
        '''
        iso_cpus = parse_cpu_list(isolated)
        

        # If no isolated CPUs, check all CPUs
        if not iso_cpus:
            # Get number of CPUs
            stdin, stdout, stderr = ssh.exec_command("nproc")
            ncpus = int(stdout.read().decode().strip())
            cpu_indices = list(range(ncpus))
        else:
            cpu_indices = iso_cpus

        # Read /proc/stat twice with a short interval
        stdin, stdout, stderr = ssh.exec_command("cat /proc/stat | grep '^cpu[0-9]' ")
        stat1 = stdout.readlines()
        time.sleep(1)
        stdin, stdout, stderr = ssh.exec_command("cat /proc/stat | grep '^cpu[0-9]' ")
        stat2 = stdout.readlines()

        cpu_times1 = {}
        cpu_times2 = {}
        for line in stat1:
            parts = line.strip().split()
            cpu = int(parts[0][3:])
            times = list(map(int, parts[1:]))
            cpu_times1[cpu] = times
        for line in stat2:
            parts = line.strip().split()
            cpu = int(parts[0][3:])
            times = list(map(int, parts[1:]))
            cpu_times2[cpu] = times

        for cpu in cpu_indices:
            t1 = cpu_times1.get(cpu)
            t2 = cpu_times2.get(cpu)
            if not t1 or not t2:
                status = 'Unknown'
            else:
                # Calculate idle time for the CPU at two time points.
                # t1[3] is 'idle', t1[4] (if present) is 'iowait' from /proc/stat fields.
                idle1 = t1[3] + (t1[4] if len(t1) > 4 else 0)
                idle2 = t2[3] + (t2[4] if len(t2) > 4 else 0)

                # Calculate total time (sum of all fields) at both time points.
                total1 = sum(t1)
                total2 = sum(t2)

                # Calculate the change in idle and total time between the two samples.
                idle_delta = idle2 - idle1
                total_delta = total2 - total1

                # CPU usage is the proportion of time NOT spent idle between the two samples.
                # usage = 100 * (1 - (idle_delta / total_delta))
                # If total_delta is 0 (shouldn't happen), usage is set to 0.
                usage = 100 * (1 - idle_delta / total_delta) if total_delta > 0 else 0

                # If usage is greater than 1%, consider the CPU 'Busy', else 'Idle'.
                status = 'Busy' if usage > 1 else 'Idle'  # >1% usage = busy
            results.append({
                'server': servername,
                'cpu': cpu,
                'isolated': 'yes' if cpu in iso_cpus else 'no',
                'status': status
            })
        ssh.close()
    except Exception as e:
        if logger:
            logger.error(f"Error connecting to {servername}: {e}")
        results.append({'server': servername, 'cpu': '', 'isolated': '', 'status': f'SSH connect ERROR: {e}'})
    return results

def create_server_dict_from_file(filename, debug=False, logger: Optional[logging.Logger] = None):
    """
    Reads a text file where each line is 'key,[list of servers]' and returns a dictionary.
    If debug is True, prints the resulting dictionary.
    """
    from collections import OrderedDict
    server_dict = {}
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or ',' not in line:
                continue
            key, value = line.split(',', 1)
            key_upper = key.strip().upper()
            value = value.strip()
            # Remove outer quotes if present
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            # Replace doubled quotes with single quotes
            value = value.replace('""', '"')
            try:
                server_list = ast.literal_eval(value)
                if isinstance(server_list, list):
                    if key_upper in server_dict:
                        # Merge lists, avoid duplicates
                        server_dict[key_upper].extend([s for s in server_list if s not in server_dict[key_upper]])
                    else:
                        server_dict[key_upper] = server_list
            except Exception as e:
                if debug and logger:
                    logger.warning(f"Error parsing line: {line} ({e})")
    # Sort keys and return OrderedDict
    ordered = OrderedDict(sorted(server_dict.items()))
    if debug and logger:
        for k, v in ordered.items():
            logger.info(f"{k}: {v}")
    return ordered

def parse_lscpu_output(lscpu_lines):
    import re
    sockets = 0
    socket_cpu_sets = {}
    lscpu_list = lscpu_lines.split('\n')
    iso_cpus = ''
    # Parse isolated CPUs from the first line if present
    if lscpu_list and re.match(r'^[0-9,-]+$', lscpu_list[0].replace(' ', '')):
        iso_cpus = lscpu_list[0].replace(' ', '')
        lscpu_info_start = 1
    else:
        iso_cpus = ''
        lscpu_info_start = 0

    # Parse socket info from lscpu output
    # Look for lines like: "Socket(s):           2"
    for line in lscpu_list[lscpu_info_start:]:
        if line.strip().startswith('Socket(s):'):
            try:
                sockets = int(line.split(':')[1].strip())
            except Exception:
                sockets = 0
            break
    else:
        sockets = 0

    # Parse per-CPU/socket mapping from any line with two integers (e.g., '0 0', '47 1', ...)
    if iso_cpus == '':
        socket_cpu_sets = None
    else:
        for x in range(sockets):
            s = ''.join(lscpu_list[x+3][lscpu_list[x+3].find(':')+1:].strip().split())
            start, end = map(int, s.split('-'))
            cpus = list(range(start, end + 1))
            socket_cpu_sets[x] = cpus

    return sockets, socket_cpu_sets, iso_cpus

def process_server(
    idx, servername, team_key, user, serverDict_details,
    logger: Optional[logging.Logger] = None
):
    if servername[:3].upper() != 'TA-' and servername[:3].upper() != 'AC-':
        if logger:
            logger.info(f"Skipping {servername} as it does not start with 'TA-' or 'AC-'.")
        return None
    if logger:
        logger.info(f"Checking {servername} ({team_key}) as {user}...")
    results = []
    if servername == 'TA-TKY-FIX-A-01':
        if logger:
            logger.info(f"Skipping {servername} due to known SSH issues.")
    # Get CPUs per socket using lscpu -e
    #lscpu_cmd = "lscpu -e=CPU,SOCKET | awk 'NR>1 {print $1, $2}'"
    lscpu_cmd = "lscpu | grep -E 'Socket|NUMA'"
    # Get isolated CPUs using the same method as in get_cpu_idle_status
    iso_cmd = "cat /sys/devices/system/cpu/isolated 2>/dev/null || grep -o 'isolcpus=[^ ]*' /proc/cmdline | cut -d= -f2"
    iso_lscpu_cmd = f"{iso_cmd} && {lscpu_cmd}"
    socket0_set = set()
    socket1_set = set()
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())        
        ssh.connect(hostname=servername, username=user, timeout=5)
        stdin, stdout, stderr = ssh.exec_command(iso_lscpu_cmd)
        cpu_socket_lines = stdout.read().decode().strip().split('\n')
        sockets, socket_cpu_sets, iso_cpus = parse_lscpu_output('\n'.join(cpu_socket_lines))
        # If no isolated CPUs, we will check all CPUs and group by socket based on lscpu output. If lscpu output does not provide socket info, we will just check all CPUs without socket grouping.
        if not socket_cpu_sets:
            results.append({'status': f"ERROR Parsed lscpu output: no isolated CPUs, {sockets} sockets"})
        ''' 
            results are in the form [{'server': '...', 'cpu': 0, 'isolated': 'yes/no', 'status': 'Busy/Idle'}, ...] 
            Current get_cpu_idle_status checks for isolated CPUs but does not group by socket, so we need to determine which CPUs belong to which socket and then calculate busy/idle percentages per socket.
            If no isocpus are found, we check all CPUs and group them by socket using the lscpu output. Then we can calculate the percentage of busy and idle CPUs for each socket and include that in the final output.
        '''
        results = get_cpu_idle_status(servername, ssh, iso_cpus, logger=logger)
        if results and 'ERROR' in results[0]['status']:
            if logger:
                logger.error(f"Error checking {servername}: {results[0]['status']}")
            row = [idx, servername, '', team_key, '', '', '', '', '', '', '', '', results[0]['status'], '', '', '']
            return row   
        
        ssh.close()
        
       
        
        socket0_set = socket_cpu_sets.get(0, set())
        socket1_set = socket_cpu_sets.get(1, set())
    except Exception as e:
        if logger:
            logger.error(f"Error connecting to {servername}: {e}")
        results.append({'server': servername, 'cpu': '', 'isolated': '', 'status': f'SSH connect ERROR: {e}'})
        status = results[0]['status'] if results else f'SSH connect ERROR: {e}'
        row = [idx, servername, '', team_key, '', '', '', '', '', '', '', '', status, '', '', '']
        return row
    

    # If only a single socket, set all Socket1 values to 'n/a'
    #single_socket = len(socket1_set) == 0


    # Group CPUs by socket
    busy_socket0 = []
    busy_socket1 = []
    idle_socket0 = []
    idle_socket1 = []

    for r in results:
        cpu = r['cpu']
        if cpu in socket0_set:
            if r['status'] == 'Busy':
                busy_socket0.append(str(cpu))
            elif r['status'] == 'Idle':
                idle_socket0.append(str(cpu))
        elif cpu in socket1_set:
            if r['status'] == 'Busy':
                busy_socket1.append(str(cpu))
            elif r['status'] == 'Idle':
                idle_socket1.append(str(cpu))

    total_socket0 = len(busy_socket0) + len(idle_socket0)
    total_socket1 = len(busy_socket1) + len(idle_socket1)
    percent_busy_socket0 = (len(busy_socket0) / total_socket0 * 100) if total_socket0 > 0 else 0
    percent_free_socket0 = 100 - percent_busy_socket0 if total_socket0 > 0 else 0
    if not sockets:
        percent_busy_socket0 = 'n/a'
        percent_free_socket0 = 'n/a'
        busy_socket0 = ['n/a']
        idle_socket0 = ['n/a']
        percent_busy_socket1 = 'n/a'
        percent_free_socket1 = 'n/a'
        busy_socket1 = ['n/a']
        idle_socket1 = ['n/a']
    elif sockets == 1:
        percent_busy_socket1 = 'n/a'
        percent_free_socket1 = 'n/a'
        busy_socket1 = ['n/a']
        idle_socket1 = ['n/a']
    else:
        percent_busy_socket1 = (len(busy_socket1) / total_socket1 * 100) if total_socket1 > 0 else 0
        percent_free_socket1 = 100 - percent_busy_socket1 if total_socket1 > 0 else 0

    
    server_details = serverDict_details
    aws_az = servername[3:8] if servername[:3].upper() == 'TA-' or servername[:3].upper() == 'AC-' else server_details.get(servername, [{}])[0].get('aws_az', '')
    owner = server_details.get(servername, [{}])[0].get('owner', '')
    instance_type = server_details.get(servername, [{}])[0].get('instance_type', '')
    def fmt(val):
        return f'{val:.2f}' if isinstance(val, float) else val

    row = [
        idx, servername, aws_az, team_key, owner, instance_type,
        sockets,
        str(iso_cpus),
        fmt(percent_busy_socket0), fmt(percent_busy_socket1),
        fmt(percent_free_socket0), fmt(percent_free_socket1),
        ','.join(busy_socket0), ','.join(busy_socket1),
        ','.join(idle_socket0), ','.join(idle_socket1)
    ]
    if logger:
        logger.info(
            f"{idx}, {servername}, {aws_az}, {team_key}, owner: {owner}, instance_type: {instance_type}, "
            f"sockets: {sockets}, iso_cpus: {iso_cpus}, "
            f"%Busy_Socket0: {fmt(percent_busy_socket0)}, %Busy_Socket1: {fmt(percent_busy_socket1)}, "
            f"%Free_Socket0: {fmt(percent_free_socket0)}, %Free_Socket1: {fmt(percent_free_socket1)}, "
            f"Busy_Socket0: {','.join(busy_socket0)}, Busy_Socket1: {','.join(busy_socket1)}, "
            f"Free_Socket0: {','.join(idle_socket0)}, Free_Socket1: {','.join(idle_socket1)}"
        )
    return row

def create_server_dict_from_pg_query_result(
    pg_result, debug=False, logger: Optional[logging.Logger] = None
):
    """
    Transforms the PostgreSQL query result into a dictionary of the form {team_key: [[{host:server1},{owner:owner1}], [{host:server2},{owner:owner2}], ...]]}.
    The team_key is derived from the tags which is list of dictionaries. Team  (e.g., 'TA-SEO-B-07' -> 'SEO').
    and example of the list of dictonaries in the tags column is [{'key': 'team', 'value': 'MM'}, {'key': 'owner', 'value': 'Adam'}]
    the function iterates through the PostgreSQL query result, extracts the team from the tags, and organizes the servers into a 
    dictionary where each key is a team and the value is a list of lists with server names and Owner  owner is pulled from the tags and server is the key of dictionary passed in.
    If debug is True, prints the resulting dictionary.
    """
    server_dict = {}
    for server, details in pg_result.items():
        tags = details[2] if len(details) > 1 else {}
        team = tags.get('Team','').upper()
        owner = tags.get('Owner','')
        instance_type = details[1] if len(details) > 1 else ''
        instance_id = details[0] if len(details) > 0 else ''
        pro_core_cnt = details[3] if len(details) > 3 else ''
        if server:
            if server not in server_dict:
                server_dict[server] = []
            server_dict[server].append({'host': server, 'instance_id': instance_id, 'instance_type': instance_type, 'team': team, 'owner': owner, 'pro_core_cnt': pro_core_cnt})
        else:
            if logger:
                logger.warning(f"Warning: No team tag found for server {server}. Skipping.")
    if debug and logger:
        for team, servers in server_dict.items():
            logger.info(f"{team}: {servers}")
    return server_dict

def _write_refresh_status(status: str, error: str = ''):
    """Write refresh status file when invoked by GUI (REFRESH_STATUS_FILE env)."""
    path = os.environ.get('REFRESH_STATUS_FILE')
    if not path:
        return
    try:
        import datetime
        dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"{status}|{dt}"
        if error:
            line += f"|{error}"
        with open(path, 'w') as f:
            f.write(line)
    except Exception:
        pass


def main():
    logger = create_logger(__name__)
    start_time = time.time()
    pg_aws_Query = """SELECT title, instance_id, instance_type, tags, cpu_options_core_count 
                        FROM steampipe_cache.aws_ec2_instance 
                        WHERE title LIKE 'TA-%' AND instance_state = 'running' AND tags NOTNULL ORDER BY title;"""
    pg_ali_Query = """SELECT name, instance_id, instance_type, tags, cpu_options_core_count 
                        FROM steampipe_cache.alicloud_ecs_instance 
                        WHERE name LIKE 'AC-%' AND Status ='Running' AND tags NOTNULL ORDER BY name ;
                    """
    pgRunner = PostgresQueryRunner(
        PostgresQueryRunner.load_db_creds_from_file(".DBCreds.json"),
        logger=logger
    )
    aws = pgRunner.run_query(pg_aws_Query, key='title')
    ali = pgRunner.run_query(pg_ali_Query, key='name')
    serverDict2 = create_server_dict_from_pg_query_result(ali | aws, logger=logger)
    team_dict = defaultdict(list)
    team_dict = {}
    for details_list in serverDict2.values():
        for details in details_list:
            team = details.get('team')
            host = details.get('host')
            if team:
                team_dict.setdefault(team, []).append(host)
    
    teamsList = sorted(["TAO", "OMNIA", "FZE", "ARB", "PD", "DEFI", "MM", "RWD", "OTC", "TAKE", "DLP", "DPDK"])
    all_server_rows = []
    user = 'archy'  # Replace with your username

    # Build a flat list of (team_key, server) pairs across all teams.
    # This allows us to assign a globally unique 'num' index to each server, regardless of team.
    all_team_server_pairs = []
    team_to_servers = defaultdict(list)
    
    for servername, details in serverDict2.items():
        team = details[0].get('team')
        if team in teamsList:
            team_to_servers[team].append(servername)
            all_team_server_pairs.append((team, servername))
        #check for shared servers and if a shared server add that name as a team
        if '+' in team:
            sub_teams = team.split('+')
            for sub_team in sub_teams:
                sub_team = sub_team.strip()
                if sub_team in teamsList:
                    team_to_servers[team].append(servername)
                    all_team_server_pairs.append((team, servername))
                    #only add the server once with the full team name (e.g., "TAO+OMNIA") but not with the individual sub-team names to avoid duplicates in the output. The full team name will be included in the 'team' column of the output CSV, and we can see which servers are shared by looking for team names that contain '+'. If we added the server multiple times under each sub-team, it would inflate the count of servers for those teams and make it harder to identify shared servers.
                    break
        
    # Enumerate globally so that 'num' is unique across all servers (not per team).
    # All SSH work is handled by the Go binary (ssh_cpu_check), which runs every
    # server concurrently via goroutines instead of 7-at-a-time Python threads.
    go_binary = os.path.join(os.path.dirname(__file__), 'ssh_cpu_check')
    go_input = {
        'user': user,
        'servers': [
            {'idx': idx, 'server': server, 'team': team_key}
            for idx, (team_key, server) in enumerate(all_team_server_pairs, 1)
        ],
    }
    proc = subprocess.run(
        [go_binary],
        input=json.dumps(go_input).encode(),
        capture_output=True,
        timeout=300,
    )
    if proc.returncode != 0:
        err_msg = proc.stderr.decode(errors='replace')
        logger.error(f"Go subprocess failed (exit {proc.returncode}): {err_msg}")
        raise RuntimeError(f"ssh_cpu_check error: {err_msg}")
    # Forward Go's stderr (progress/error lines) to our logger.
    for line in proc.stderr.decode(errors='replace').splitlines():
        if line.strip():
            logger.info(f"[go] {line}")

    go_output = json.loads(proc.stdout)
    for r in go_output['results']:
        if r.get('error', '').startswith('SKIP'):
            continue
        server = r['server']
        idx = r['idx']
        team_key = r['team']
        details = serverDict2.get(server, [{}])
        d = details[0] if details else {}
        aws_az = server[3:8] if server[:3].upper() in ('TA-', 'AC-') else d.get('aws_az', '')
        owner = d.get('owner', '')
        instance_type = d.get('instance_type', '')
        if r.get('error'):
            row = [idx, server, '', team_key, '', '', '', '', '', '', '', '', r['error'], '', '', '']
        else:
            row = [
                idx, server, aws_az, team_key, owner, instance_type,
                r['sockets'], r['iso_cpus'],
                r['pct_busy_socket0'], r['pct_busy_socket1'],
                r['pct_free_socket0'], r['pct_free_socket1'],
                r['busy_socket0'], r['busy_socket1'],
                r['idle_socket0'], r['idle_socket1'],
            ]
        all_server_rows.append(row)
    end_time = time.time() - start_time
    logger.info(f"Total servers to checked: {len(all_team_server_pairs)}")
    logger.info(f"Completed in {end_time:.2f} seconds.")
    # Sort all_server_rows by the 'num' column (index 0)
    all_server_rows_sorted = sorted(all_server_rows, key=lambda x: x[0])
    # Write all results to CSV
    if len(all_server_rows_sorted) == 0:
        logger.warning("No server data collected. CSV file will not be created.")
        _write_refresh_status('error', 'No server data collected')
        return
    with open(OUTPUT_CSV, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            'num', 'server name', 'AZ', 'team', 'owner', 'instance_type',
            'sockets', 'iso_cpus',
            '%Busy_Socket0', '%Busy_Socket1', '%Free_Socket0', '%Free_Socket1',
            'Busy_Socket0', 'Busy_Socket1', 'Free_Socket0', 'Free_Socket1'
        ])
        for row in all_server_rows_sorted:
            writer.writerow(row)
    logger.info(f"Results written to {OUTPUT_CSV}")
    _write_refresh_status('idle')


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        _write_refresh_status('error', str(e))
        raise