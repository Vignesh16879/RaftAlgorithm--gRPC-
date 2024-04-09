


def get_nodes():
    file_path = 'nodes.txt'
    nodes = {}
    
    with open(file_path, 'r') as file:
        lines = file.readlines()
        
        for line in lines:
            parts = line.strip().split(':', 1)
            
            if len(parts) == 2:
                node_id = parts[0].strip().strip("'")
                address = parts[1].strip().strip("'")
                nodes[node_id] = address
    
    return nodes


def main():
    nodes = get_nodes()


if __name__ == "__main__":
    main()