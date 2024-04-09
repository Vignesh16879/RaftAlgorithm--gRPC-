# RaftAlgorithm--gRPC-

This project implements the Raft consensus algorithm using gRPC for communication between nodes. Raft is designed to ensure strong consistency in a distributed system by electing a leader among nodes, handling failures, and replicating logs across the cluster.

## Features

- **SET**: Set a key-value pair in the distributed key-value store.
- **GET**: Retrieve the value associated with a key from the distributed key-value store.
- **NO-OP**: Perform a no-operation command for testing purposes.
- **Heartbeats**: Nodes send periodic heartbeats to indicate their status and prevent leader election timeouts.
- **Append Entries**: Leaders send log entries to followers to replicate the log.
- **Handling Broken Nodes**: The algorithm handles broken nodes gracefully, electing new leaders as needed.
- **Election**: Nodes participate in leader election when the current leader fails or communication breaks down.

## Usage

### Requirements
- Python 3.x
- gRPC (install using `pip install grpcio`)

### Running the RaftAlgorithm

1. Clone the repository:
    ```bash
    git clone https://github.com/your_username/RaftAlgorithm--gRPC-.git
    ```

2. Navigate to the project directory:
    ```bash
    Copy code
    cd RaftAlgorithm--gRPC-
    ```

3. Start the server:
bash
python3 server.py <number_of_nodes>

4. Start each node (replace the arguments accordingly):
    ```bash
    # Node-0:
    python3 node.py '000' "{{'000' : {'port' : [::]:7000, 'address' : 'localhost' },{'001' : {'port' : [::]:7001, 'address' : 'localhost' },{'002' : {'port' : [::]:7002, 'address' : 'localhost' },}"
    # Node-1:
    python3 node.py "001" "{{'000' : {'port' : [::]:7000, 'address' : 'localhost' },{'001' : {'port' : [::]:7001, 'address' : 'localhost' },{'002' : {'port' : [::]:7002, 'address' : 'localhost' },}"
    # Node-2:
    python3 node.py "002" "{{'000' : {'port' : [::]:7000, 'address' : 'localhost' },{'001' : {'port' : [::]:7001, 'address' : 'localhost' },{'002' : {'port' : [::]:7002, 'address' : 'localhost' },}"
    ```

5. Run the client (replace <client_id> with an integer):
    ```bash
    python3 client.py <client_id>
    ```