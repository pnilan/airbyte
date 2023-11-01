import argparse
import json
import sys
import os
import requests
from datetime import date
from datetime import datetime
from datetime import timedelta

# check method
def _call_api (ticker, token):
  time_format = '%Y-%m-%d'
  today = date.today()
  to_day = today.strftime(time_format)
  from_day = (today - timedelta(days=7)).strftime(time_format)
  return requests.get(f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{from_day}/{to_day}?sort=asc&limit=120&apiKey={token}')

def check(config):
  # Validates input config by attempting to get daily closing prices of the input string stock ticker
  response = _call_api(ticker=config['stock_ticker'], token=config['api_key'])
  if response.status_code == 200:
    result = { 'status': 'SUCCEEDED' }
  elif response.status_code == 403:
    result = { 'status': 'FAILED', 'message': 'API Key is incorrect' }
  else:
    result = { 'status': 'FAILED', 'message': 'Input configuration is incorrect' }

  output_message = { 'type': 'CONNECTION_STATUS', 'connectionStatus': result }
  print(json.dumps(output_message))

def get_input_file_path(filepath):
  if os.path.isabs(filepath):
    return filepath
  else:
    return os.path.join(os.getcwd(), filepath)

# Spec method
def read_json(filepath):
  with open(filepath, 'r') as f:
    return json.loads(f.read())

def log(message):
  log_json = {
    'type': 'LOG',
    'log': message
  }
  print(json.dumps(log_json))

def spec():
  # Read the file named spec.json from the module directory as a JSON file
  current_script_directory = os.path.dirname(os.path.realpath(__file__))
  spec_path = os.path.join(current_script_directory, 'spec.json')
  specification = read_json(spec_path)

  # form an Airbyte Message containing the spec and print it to stdout
  airbyte_message = {
    'type': 'SPEC',
    'spec': specification
  }
  print(json.dumps(airbyte_message))

def run(args):
  parent_parser = argparse.ArgumentParser(add_help=False)
  main_parser = argparse.ArgumentParser()
  subparsers = main_parser.add_subparsers(title='commands', dest='command')

  # Accept the spec command
  subparsers.add_parser('spec', help='outputs the json config spec', parents=[parent_parser])

  # Accept the check command
  check_parser = subparsers.add_parser('check', help='checks the config used to connect', parents=[parent_parser])
  required_check_parser = check_parser.add_argument_group('required named arguments')
  required_check_parser.add_argument('--config', type=str, required=True, help='path to the json config file')

  parsed_args = main_parser.parse_args(args)
  command = parsed_args.command

  if command == 'spec':
    spec()
  elif command == 'check':
    config_file_path = get_input_file_path(parsed_args.config)
    config = read_json(config_file_path)
    check(config)
  else:
    # If we don't recognize the command log, exit with an error code greater than zero to indicate the process had a failure
    log("Invalid command. Allowable commands: [spec, check]")
    sys.exit(1)

  # A zero exit means the process successfully completed
  sys.exit(0)

def main():
  arguments = sys.argv[1:]
  run(arguments)

if __name__ == '__main__':
  main()

