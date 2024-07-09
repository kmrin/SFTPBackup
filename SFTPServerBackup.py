import os
import sys
import json
import shutil
import socket
import asyncio
import logging
import argparse
import asyncssh
import subprocess

from datetime import datetime

print(
'''
            _____ ______ _______ _____     ____             _                         
           / ____|  ____|__   __|  __ \   |  _ \           | |                        
  ______  | (___ | |__     | |  | |__) |  | |_) | __ _  ___| | ___   _ _ __    ______ 
 |______|  \___ \|  __|    | |  |  ___/   |  _ < / _` |/ __| |/ / | | | '_ \  |______|
           ____) | |       | |  | |       | |_) | (_| | (__|   <| |_| | |_) |         
          |_____/|_|       |_|  |_|       |____/ \__,_|\___|_|\_\\__,_| .__/          
                                                                      | |             
                                                                      |_|             
 v1.0.0                                                                    made by rin
'''
)

localdir = f'{os.path.realpath(os.getcwd())}'

class LoggingFormatter(logging.Formatter):
    black = '\x1b[30m'
    red = '\x1b[31m'
    green = '\x1b[32m'
    yellow = '\x1b[33m'
    blue = '\x1b[34m'
    gray = '\x1b[38m'
    reset = '\x1b[0m'
    bold = '\x1b[1m'

    COLORS = {
        logging.DEBUG: gray + bold,
        logging.INFO: blue + bold,
        logging.WARNING: yellow + bold,
        logging.ERROR: red,
        logging.CRITICAL: red + bold
    }

    def format(self, record):
        log_color = self.COLORS.get(record.levelno)

        format_str = '(black){asctime}(reset) (levelcolor){levelname:<8}(reset) (green){name}(reset) {message}'
        format_str = format_str.replace('(black)', self.black + self.bold)
        format_str = format_str.replace('(reset)', self.reset)
        format_str = format_str.replace('(levelcolor)', log_color)
        format_str = format_str.replace('(green)', self.green + self.bold)

        formatter = logging.Formatter(format_str, '%d-%m-%y %H:%M:%S', style='{')

        return formatter.format(record)

timestamp = datetime.now().strftime('%d.%m.%y-%H%M%S')
log_filename = os.path.join(os.path.join(localdir, 'logs', f'mc-backup-{timestamp}.log'))
log_dirname = os.path.dirname(log_filename)

os.makedirs(log_dirname, exist_ok=True)

logger = logging.getLogger('SFTP-Backup')
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(LoggingFormatter())

file_handler = logging.FileHandler(
    filename=log_filename,
    mode='a',
    encoding='utf-8',
    delay=False
)

file_handler.setFormatter(
    logging.Formatter(
        "[{asctime}] [{levelname:<8}] {name}: {message}", "%Y-%m-%d %H:%M:%S",
        style="{"
    )
)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

cache_dir = os.path.join(os.getcwd(), 'cache')
os.makedirs(cache_dir, exist_ok=True)

parser = argparse.ArgumentParser(description='SFTP Server Backup')
parser.add_argument('--dir', help='Directory to save files to (working directory if left empty)')
parser.add_argument('--retries', help='The amount of times the script should add a numbered sufix to the filename in case a similar file exists')
args = parser.parse_args()

if args.retries:
    try:
        retry_limit_arg = int(args.retries)
    except ValueError:
        logger.error('--retries argument must be an integer')
        sys.exit(1)

retry_limit = args.retries if args.retries else 10
retry_counter = 0

try:
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
except FileNotFoundError:
    logger.error('config.json file not found')
    sys.exit(1)

try:
    archive_name = config['archive_name']
    sftp_config = config['sftp_config']
    data = config['data']
except KeyError as e:
    logger.error(f'Could not extract data from config.json: {e.args}')
    sys.exit(1)

def cleanup(success: bool = False) -> None:
    logger.info('Clearing cache')
    
    shutil.rmtree(cache_dir)
    
    if not success:
        sys.exit(1)

def get_filename() -> str:
    timestamp = datetime.now().strftime('%d.%m.%y-%H%M')
    name = f'{archive_name}-{timestamp}'
    
    return name

def run_command(command: str) -> None:
    subprocess.run(
        command, check=True, cwd=cache_dir,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

def move_archive(backup_dir: str, filename: str, retry: bool = False) -> str:
    global retry_counter
    
    if retry:
        retry_counter += 1
        logger.warning(f'Backup file already exists, adding numbered sufix to filename: {retry_counter}')
        
        if retry_counter >= retry_limit:
            logger.error(f'Amount of numbered sufix limit reached: {retry_limit}')
            cleanup()
        
        final_filename = f'{filename}({retry_counter})'
    
    final_filename = f'{filename}.7z' if not retry else f'{final_filename}.7z'
    filepath = os.path.join(backup_dir, final_filename)
    
    try:
        shutil.move(os.path.join(cache_dir, f'{filename}.7z'), filepath)
    except FileExistsError:
        move_archive(backup_dir, filename, retry=True)
    
    return filepath

async def main(backup_dir: str) -> None:
    try:
        async with asyncssh.connect(**sftp_config) as conn:
            logger.info(f'Connecting [HOST: {sftp_config["host"]} | PORT: {sftp_config["port"]}]')
            
            async with conn.start_sftp_client() as sftp:
                logger.info('Retrieving data (this might take a while)')
                
                try:
                    for info in data:
                        logger.info(f'Retrieving "{info}"')
                        await sftp.get([info], localpath='cache', recurse=True)
                except asyncssh.sftp.SFTPNoSuchFile:
                    logger.error(f'No such file/path: "{info}"')
                    cleanup()
                except asyncssh.sftp.SFTPNoSuchPath:
                    logger.error(f'No such file/path: "{info}"')
                    cleanup()
                except Exception as e:
                    logger.error(f'Could not finish data retrieval: {e}')
                    cleanup()
    except socket.gaierror as e:
        logger.error(f'Connection error: {e.args}')
        cleanup()
    except asyncssh.misc.PermissionDenied as e:
        logger.error(e.reason)
        cleanup()
    
    is_windows = sys.platform == 'win32'
    if is_windows:
        executable = os.path.join(os.path.realpath(os.path.dirname(__file__)), '7z', 'win', '7za.exe')
    else:
        executable = os.path.join(os.path.realpath(os.path.dirname(__file__)), '7z', 'linux', '7zz')
    
    filename = get_filename()
    command = [executable, 'a', '-t7z', f'{filename}.7z', *data]
    
    logger.info('Creating archive')
    
    if not is_windows:
        run_command(['chmod', '+x', f'{executable}'])
    
    run_command(command)
                
    logger.info('Moving backup')
    archive_path = move_archive(backup_dir, filename)
    
    cleanup(success=True)
    
    print(f'\nFinished!\nBackup saved: "{archive_path}"\n')
    
    sys.exit(1)

if __name__ == '__main__':
    if not args.dir:
        logger.error('Output directory not specified')
        sys.exit(1)
    
    asyncio.run(main(args.dir))
