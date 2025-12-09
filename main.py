import os
import subprocess
import configparser
import sys
import argparse

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Daily Stats ETL Main Script')
    parser.add_argument('--client-ids', 
                      nargs='+',
                      type=int,
                      help='List of client IDs to process (e.g., --client-ids 10007 10008)')
    parser.add_argument('--script',
                      type=str,
                      help='Specific script to run (e.g., default, xpay, uprev)')
    return parser.parse_args()

# Parse command line arguments
args = parse_arguments()

# Load config file
config = configparser.ConfigParser()
config.read('config.ini')

# List of available scripts
scripts = ['default', 'detailed', 'joins', 'blueastro', 'enormous', 'xpay', 'xpay_ss', 'ecomm', 'ecomm_2', 'xpay_rebill', 'xpay_payfac', 'uprev', 'marketnice_10050', 'xpay_weekly', 'ecomm_10049', 'xpay_payfac_daily']

# Determine which scripts to run
scripts_to_run = []
if args.script:
    # Run only the specified script
    if args.script in scripts:
        scripts_to_run = [args.script]
    else:
        print(f"Error: Script '{args.script}' not found. Available scripts: {', '.join(scripts)}")
        sys.exit(1)
else:
    # Run all enabled scripts from config
    scripts_to_run = [script for script in scripts if config['General'].getboolean(f'run_{script}')]

# Execute scripts
for script in scripts_to_run:
    script_path = os.path.join('scripts', script, 'index.py')
    print(f"🚀 Executing {script_path}...")
    
    # Build command with client IDs if provided
    cmd = [sys.executable, script_path]
    if args.client_ids:
        cmd.extend(['--client-ids'] + [str(client_id) for client_id in args.client_ids])
    
    subprocess.run(cmd)
