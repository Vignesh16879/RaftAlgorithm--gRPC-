import os
import sys
import time
import signal
import logging
import subprocess
from pathlib import Path


BASE_DIR = Path.cwd()
logging.basicConfig(filename = "server.log", format='%(asctime)s %(message)s', filemode = 'w')
LOG = logging.getLogger()
LOG.setLevel(logging.DEBUG)


def set_nodes(n = 3):
    entries = {}
    
    for i in range(n):
        node_id = str(i).zfill(3)  
        address = f'[::]:7{node_id}' 
        entries[node_id] = address
    
    file_path = 'nodes.txt'
    
    with open(file_path, 'w') as file:
        for node_id, address in entries.items():
            line = f"'{node_id}' : '{address}'\n"
            file.write(line)


def print_node_commands():
    file_path = 'nodes.txt'
    entries = {}
    array = "{"
    nodes = []
    
    with open(file_path, 'r') as file:
        lines = file.readlines()
        
        for line in lines:
            parts = line.strip().split(':', 1)
            
            if len(parts) == 2:
                node_id = parts[0].strip().strip("'")
                address = parts[1].strip().strip("'")
                entries[node_id] = address
                nodes.append(node_id)
                array += "{" + f"'{node_id}' : " + "{" + f"'port' : {address}, 'address' : 'localhost' " + "},"
    
    array += "}"
    
    for node in nodes:
        print(f"python3 node.py '{node}' " + '"' f"{array}" + '"')


def server():
    pass


def main():
    temp = "{'000': {'port' : '[::]7000', 'address' : 'localhost:7000' }}"
    set_nodes()
    print_node_commands()
    pass


if __name__ == "__main__":
    main()