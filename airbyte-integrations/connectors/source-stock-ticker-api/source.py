import argparse
import json
import sys
import os

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

  parsed_args = main_parser.parse_args(args)
  command = parsed_args.command

  if command == 'spec':
    spec()
  else:
    # If we don't recognize the command log, exit with an error code greater than zero to indicate the process had a failure
    log("Invalid command. Allowable commands: [spec]")
    sys.exit(1)

def main():
  arguments = sys.argv[1:]
  run(arguments)

if __name__ == '__main__':
  main()