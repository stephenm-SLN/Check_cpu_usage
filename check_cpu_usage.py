import paramiko
import csv
import os
import ast
from typing import List, Dict
import boto3
import requests

SSH_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config')
OUTPUT_CSV = 'cpu_usage.csv'
ALI_ECS_INVENTORY_URL = 'http://inventory.devops.selini.tech/alicloud/ecs'  # Replace with actual URL

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

def get_cpu_idle_status(servername: str, user: str) -> List[Dict[str, str]]:
    results = []
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=servername, username=user)

        # Get isolated CPUs
        iso_cmd = "cat /sys/devices/system/cpu/isolated 2>/dev/null || grep -o 'isolcpus=[^ ]*' /proc/cmdline | cut -d= -f2"
        stdin, stdout, stderr = ssh.exec_command(iso_cmd)
        isolated = stdout.read().decode().strip() or ''
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
        results.append({'server': servername, 'cpu': '', 'isolated': '', 'status': f'ERROR: {e}'})
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

def main():
    #servers = parse_ssh_config(SSH_CONFIG_PATH)
    serverDict = create_server_dict_from_file('server_list.txt', debug=True)
    teamsList = sorted(["TAO", "OMNIA", "FZE", "ARB", "PD", "DEFI", "MM", "RWD", "OTC", "TAKE", "DLP", "DPDK"])
    all_server_rows = []
    user = 'archy'  # Replace with your username
    server_AWS_details = get_all_servers_by_name_tag(owner_tag='Owner', debug=True)
    server_ALI_details = fetch_ecs_inventory(ALI_ECS_INVENTORY_URL)
    for team in teamsList:
        team_key = team.upper()
        if team_key not in serverDict:
            print(f"team {team_key} not found in serverDict.")
            continue
        for idx, server in enumerate(serverDict[team_key], 1):
            if  server[:3].upper() != 'TA-' and server[:3].upper() != 'AC-':
                print(f"Skipping {server} as it does not start with 'TA-' or 'AC-'.")
                continue
            print(f"Checking {server} ({team_key}) as {user}...")
            results = get_cpu_idle_status(server, user)

            busy_cpus = [str(r['cpu']) for r in results if r['status'] == 'Busy']
            idle_cpus = [str(r['cpu']) for r in results if r['status'] == 'Idle']
            total = len(busy_cpus) + len(idle_cpus)
            percent_busy = (len(busy_cpus) / total * 100) if total > 0 else 0
            percent_free = 100 - percent_busy if total > 0 else 0
            if server.startswith('TA-'):
                server_details = server_AWS_details
            elif server.startswith('AC-'):
                server_details = server_ALI_details
            # Determine AWS_AZ if server name starts with 'TA-'
            aws_az = server[3:8] if server[:3].upper() == 'TA-' or server[:3].upper() == 'AC-' else server_details.get(server, [{}])[0].get('aws_az', '')
            owner = server_details.get(server, [{}])[0].get('owner', '') 
            instance_type = server_details.get(server, [{}])[0].get('instance_type', '')
            row = [idx, server, aws_az, team_key, owner, instance_type, f'{percent_busy:.2f}', f'{percent_free:.2f}', ','.join(busy_cpus), ','.join(idle_cpus)]
            all_server_rows.append(row)
            # Print to stdout
            print(f"{idx}, {server}, {aws_az}, {team_key}, owner: {owner}, instance_type: {instance_type}, %Busy: {percent_busy:.2f}, %Free: {percent_free:.2f}, Busy: {','.join(busy_cpus)}, Idle: {','.join(idle_cpus)}")

    # Write all results to CSV
    with open(OUTPUT_CSV, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['num', 'server name', 'AWS_AZ', 'team', 'owner', 'instance_type', '%Busy', '%Free', 'Busy', 'Idle'])
        for row in all_server_rows:
            writer.writerow(row)
    print(f"Results written to {OUTPUT_CSV}")

if __name__ == '__main__':
    main()