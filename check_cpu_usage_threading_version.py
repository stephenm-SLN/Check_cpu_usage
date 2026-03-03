import paramiko
import csv
import os
import ast
from typing import List, Dict
import boto3
import requests
import concurrent.futures
import time
import re

SSH_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config')
OUTPUT_CSV = 'Threaded_cpu_usage.csv'
ALI_ECS_INVENTORY_URL = 'http://inventory.devops.selini.tech/alicloud/ecs'  # Replace with actual URL


import psycopg2
from psycopg2.extras import RealDictCursor

# Class to run a PostgreSQL query and return results as a dictionary
class PostgresQueryRunner:
    def __init__(self, host, database, user, password, port=5432, logger=None):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.logger = logger

    def run_query(self, select_statement: str) -> dict:
        """
        Executes a SELECT statement and returns a dictionary with 'id' as key and list of row values as value.
        """
        conn = None
        result = {}
        try:
            conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(select_statement)
                rows = cur.fetchall()
                for row in rows:
                    row_dict = dict(row)
                    row_id = row_dict.get('id')
                    # Remove 'id' from the values list
                    values = [v for k, v in row_dict.items() if k != 'id']
                    result[row_id] = values
        except Exception as e:
            logging.error(f"Postgres query failed: {e}")
        finally:
            if conn:
                conn.close()
        return result

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

import time
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

def get_cpu_idle_status(servername: str, ssh, isolated) -> List[Dict[str, str]]:
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
        print(f"Error connecting to {servername}: {e}")
        results.append({'server': servername, 'cpu': '', 'isolated': '', 'status': f'SSH connect ERROR: {e}'})
    return results 

def create_server_dict_from_file(filename, debug=False):
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
                if debug:
                    print(f"Error parsing line: {line} ({e})")
    # Sort keys and return OrderedDict
    ordered = OrderedDict(sorted(server_dict.items()))
    if debug:
        for k, v in ordered.items():
            print(f"{k}: {v}")
    return ordered

def get_all_servers_by_name_tag(owner_tag='Owner', debug=False):
    """
    Returns a dict of AWS servers: {Name tag: [ {instance_id, instance_type, owner, aws_az}, ... ]} for all regions.
    """
    ec2 = boto3.client('ec2')
    regions = [r['RegionName'] for r in ec2.describe_regions()['Regions']]
    all_servers = {}
    for region in regions:
        ec2_reg = boto3.client('ec2', region_name=region)
        paginator = ec2_reg.get_paginator('describe_instances')
        for page in paginator.paginate():
            for reservation in page['Reservations']:
                for instance in reservation['Instances']:
                    name = next((tag['Value'] for tag in instance.get('Tags', []) if tag['Key'] == 'Name'), None)
                    owner = next((tag['Value'] for tag in instance.get('Tags', []) if tag['Key'].lower() == owner_tag.lower()), None)
                    instance_id = instance['InstanceId']
                    instance_type = instance['InstanceType']
                    az = instance.get('Placement', {}).get('AvailabilityZone', None)
                    if name:
                        entry = {
                            'instance_id': instance_id,
                            'instance_type': instance_type,
                            'owner': owner,
                            'aws_az': az
                        }
                        if name not in all_servers:
                            all_servers[name] = []
                        all_servers[name].append(entry)
                        if debug:
                            print(f"{name}: {entry}")
    return all_servers

def fetch_ecs_inventory(url):
    '''
    Docstring for fetch_ali_ecs_inventory
    retrieves ECS inventory from the given URL and processes it into a dictionary.
    :param url: Description
    '''
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    result = {}
    name_counts = {}
    for item in data:
        base_name = item.get('name')
        tags = item.get('tags') or {}
        entry = {
            'role': tags.get('role', 'unknown'),
            'team': tags.get('team', 'unknown'),
            'instance_type': item.get('instance_type'),
            'owner': tags.get('owner', tags.get('Owner', 'unknown')),
            'exchange': tags.get('exchange', tags.get('Exchange', 'unknown')),
            'environment': tags.get('environment', tags.get('Environment', 'unknown')),
            'status': item.get('status'),
        }
        # Ensure unique key by appending -number if needed
        if base_name not in name_counts:
            name_counts[base_name] = 0
            name = base_name
        else:
            name_counts[base_name] += 1
            name = f"{base_name}-{name_counts[base_name]}"
        result.setdefault(name, []).append(entry)
    return result

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
    for x in range(sockets):
        s = ''.join(lscpu_list[x+3][lscpu_list[x+3].find(':')+1:].strip().split())
        start, end = map(int, s.split('-'))
        cpus = list(range(start, end + 1))
        socket_cpu_sets[x] = cpus

    return sockets, socket_cpu_sets, iso_cpus

def process_server(idx, servername, team_key, user, server_AWS_details, server_ALI_details):
    if servername[:3].upper() != 'TA-' and servername[:3].upper() != 'AC-':
        print(f"Skipping {servername} as it does not start with 'TA-' or 'AC-'.")
        return None
    print(f"Checking {servername} ({team_key}) as {user}...")
    results = []

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
        ssh.connect(hostname=servername, username=user)
        stdin, stdout, stderr = ssh.exec_command(iso_lscpu_cmd)
        cpu_socket_lines = stdout.read().decode().strip().split('\n')
        sockets, socket_cpu_sets, iso_cpus = parse_lscpu_output('\n'.join(cpu_socket_lines))
        ''' 
            results are in the form [{'server': '...', 'cpu': 0, 'isolated': 'yes/no', 'status': 'Busy/Idle'}, ...] 
            Current get_cpu_idle_status checks for isolated CPUs but does not group by socket, so we need to determine which CPUs belong to which socket and then calculate busy/idle percentages per socket.
            If no isocpus are found, we check all CPUs and group them by socket using the lscpu output. Then we can calculate the percentage of busy and idle CPUs for each socket and include that in the final output.
        '''
        results = get_cpu_idle_status(servername,ssh, iso_cpus)
        if results and 'ERROR' in results[0]['status']:
            print(f"Error checking {servername}: {results[0]['status']}")
            row = [idx, servername, '', team_key, '', '', '', '', '', '', '', '', results[0]['status'], '', '', '']
            return row   
        
        ssh.close()
        
       
        
        socket0_set = socket_cpu_sets.get(0, set())
        socket1_set = socket_cpu_sets.get(1, set())
        '''
        for line in cpu_socket_lines:
            if not line.strip():
                continue
            cpu_str, socket_str = line.strip().split()
            cpu = int(cpu_str)
            socket = int(socket_str)
            if socket == 0:
                socket0_set.add(cpu)
            elif socket == 1:
                socket1_set.add(cpu)
        '''
    except Exception as e:
        print(f"Error connecting to {servername}: {e}")
        results.append({'server': servername, 'cpu': '', 'isolated': '', 'status': f'SSH connect ERROR: {e}'})
        row = [idx, servername, '', team_key, '', '', '', '', '', '', '', '', results[0]['status'], '', '', '']
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

    if servername.startswith('TA-'):
        server_details = server_AWS_details
    elif servername.startswith('AC-'):
        server_details = server_ALI_details
    else:
        server_details = {}
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
    # Print to stdout with safe formatting
    def fmt(val):
        return f'{val:.2f}' if isinstance(val, float) else val
    print(f"{idx}, {servername}, {aws_az}, {team_key}, owner: {owner}, instance_type: {instance_type}, "
          f"sockets: {sockets}, iso_cpus: {iso_cpus}, "
          f"%Busy_Socket0: {fmt(percent_busy_socket0)}, %Busy_Socket1: {fmt(percent_busy_socket1)}, "
          f"%Free_Socket0: {fmt(percent_free_socket0)}, %Free_Socket1: {fmt(percent_free_socket1)}, "
          f"Busy_Socket0: {','.join(busy_socket0)}, Busy_Socket1: {','.join(busy_socket1)}, "
          f"Free_Socket0: {','.join(idle_socket0)}, Free_Socket1: {','.join(idle_socket1)}")
    return row

def main():
    #servers = parse_ssh_config(SSH_CONFIG_PATH)
    start_time = time.time()
    
    serverDict = {'MM':["TA-TKY-A-45", "TA-SEO-B-07"]}
    serverDict = {'MM':["TA-SEO-B-07"]}
    serverDict = create_server_dict_from_file('server_list_full.txt', debug=True)
    teamsList = sorted(["TAO", "OMNIA", "FZE", "ARB", "PD", "DEFI", "MM", "RWD", "OTC", "TAKE", "DLP", "DPDK"])
    all_server_rows = []
    user = 'archy'  # Replace with your username
    server_AWS_details = get_all_servers_by_name_tag(owner_tag='Owner', debug=True)
    server_ALI_details = fetch_ecs_inventory(ALI_ECS_INVENTORY_URL)

    # Build a flat list of (team_key, server) pairs across all teams.
    # This allows us to assign a globally unique 'num' index to each server, regardless of team.
    all_team_server_pairs = []
    for team in teamsList:
        team_key = team.upper()
        if team_key not in serverDict:
            print(f"team {team_key} not found in serverDict.")
            continue
        servers = serverDict[team_key]
        for server in servers:
            all_team_server_pairs.append((team_key, server))

    # Enumerate globally so that 'num' is unique across all servers (not per team)
    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
        future_to_idx_server = {
            # idx here is the global server number (1..N), not per-team
            executor.submit(process_server, idx, server, team_key, user, server_AWS_details, server_ALI_details): (idx, server)
            for idx, (team_key, server) in enumerate(all_team_server_pairs, 1)
        }
        for future in concurrent.futures.as_completed(future_to_idx_server):
            row = future.result()
            if row:
                all_server_rows.append(row)
    end_time = time.time() - start_time
    print(f"Completed in {end_time:.2f} seconds.")
    # Sort all_server_rows by the 'num' column (index 0)
    all_server_rows_sorted = sorted(all_server_rows, key=lambda x: x[0])
    # Write all results to CSV
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
    print(f"Results written to {OUTPUT_CSV}")

if __name__ == '__main__':
    main()