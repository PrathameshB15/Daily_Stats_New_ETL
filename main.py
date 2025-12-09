import os
import subprocess
import configparser
import sys

# Load config file
config = configparser.ConfigParser()
config.read('config.ini')

# List of available scripts
scripts = ['default', 'detailed', 'joins', 'blueastro', 'enormous', 'xpay', 'xpay_ss', 'ecomm', 'ecomm_2', 'xpay_rebill', 'xpay_payfac', 'uprev', 'marketnice_10050', 'xpay_weekly', 'ecomm_10049', 'xpay_payfac_daily']

# Execute only enabled scripts
for script in scripts:
    if config['General'].getboolean(f'run_{script}'):
        script_path = os.path.join('scripts', script, 'index.py')
        print(f"🚀 Executing {script_path}...")
        subprocess.run([sys.executable, script_path])
