# Check CPU Usage

This project provides a script to check CPU usage on multiple servers. It reads a list of server hostnames or IP addresses from a file and reports their CPU usage, making it useful for system administrators and DevOps engineers.

## Features
- Reads server list from `server_list.txt`
- Connects to each server and checks CPU usage
- Outputs results for easy monitoring

## Requirements
- Python 3.x
- Python packages:
   - paramiko
   - boto3
   - botocore awscrt
- SSH access to target servers (if applicable)

## Usage
1. Add your server hostnames or IP addresses to `server_list.txt`, one per line.
2. Run the script:
   ```bash
   python check_cpu_usage.py
   ```
3. View the output for CPU usage information.

## Configuration
- Edit `server_list.txt` to specify which servers to check.
- Modify `check_cpu_usage.py` if you need to change how CPU usage is checked or reported.

## License
MIT License
