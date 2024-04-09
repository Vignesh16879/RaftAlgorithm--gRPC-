import re
import sys
import tqdm
import ast
import mmh3
import grpc
import json
import time
import utils
import random
import logging
import threading
import traceback
from queue import Queue
from pathlib import Path
from random import randint
from threading import Lock
from threading import Thread
from datetime import datetime
from concurrent import futures

from commit_log import CommitLog
from hashtable import HashTable
import raft_pb2 as pb2
import raft_pb2_grpc as pb2_grpc


BASE_DIR = None
LOG = None
MyTHREAD = None


class RaftNode(pb2_grpc.RaftServiceServicer):
    def __init__(self, NODE_ID, NODES, PORT, ADDRESS):
        global LOG
        self.node_id = NODE_ID
        self.nodes = NODES
        self.port = PORT
        self.node_address = ADDRESS
        self.ports = ""
        self.node_active = True
        metadata = self.Read_Metadata()
        self.term = metadata["TERM"]
        self.commitLength = metadata["commitLength"]
        self.state = "FOLLOWER"
        self.voted = False
        self.votes = []
        self.voted_node = None
        self.ht = HashTable()
        self.leader_id = None
        self.in_elections = False
        self.election_timeout = 0
        self.election_period_ms = randint(1000, 5000)
        self.time_limit = (random.randrange(150, 301) / 1000)
        self.candidates = []
        self.current_term = 0
        self.commit_index = 0
        self.next_indices = [0]*len(self.nodes)
        self.commit_log = CommitLog(file=f"logs_node_{self.node_id}/log.txt")
        self.CANDIDATE_THREADS = []
        self.LEADER_THREADS = []
        self.lastApplied = 0
        self.lastLogTerm = 0
        self.ENTRIES = {}
        self.LOGS = []
        self.nextIndex = [] 
        self.matchIndex = []
        self.matchTerm = []
        self.n_logs_replicated = 1
        self.candidateLastLogIndex = 0
        self.heartbeat_period = 1
        self.lease_duration = 5
        self.last_heartbeat_time = time.time()
        heartbeat_thread = threading.Thread(target = self.SendHeartbeats)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()
        self.StartNode()
    
    
    def SendHeartbeats(self):
        while True:
            current_time = time.time()
            
            if current_time - self.last_heartbeat_time >= self.heartbeat_period:
                self.last_heartbeat_time = current_time
                self.send_heartbeat_to_followers()
            
            time.sleep(1) 
    
    
    def send_heartbeat_to_followers(self):
        last_index, last_term = self.commit_log.get_last_index_term()
        
        for node_id, node_info in self.nodes.items():
            if node_id != self.node_id:
                heartbeat_msg = f"HEARTBEAT {self.node_id} {self.current_term} {last_index} {last_term}"
                response = self.append_entries(heartbeat_msg, "NO-OP")
                
                if response:
                    current_time = time.time()
                    if current_time - self.last_heartbeat_time > self.lease_duration:
                        self.Step_Down(self.term)
    
    
    def Read_Metadata(self):
        global BASE_DIR, LOG
        file = BASE_DIR / f"logs_node_{self.node_id}" / "metadata.txt"
        metadata = {}
        
        try:
            file.exits()
            
            with open(file, "r") as f:
                metadata = ast.literal_eval(f.read())
        except:
            metadata = {
                "NODE_ID" : f"{self.node_id}",
                "TERM" : 0,
                "commitLength" : 0
            }
        
        return metadata
    
    
    def Write_Metadata(self):
        global BASE_DIR, LOG
        file = BASE_DIR / f"logs_node_{self.node_id}" / "metadata.txt"
        metadata = {}
        info = "Writing to metadata.txt."
        LOG.info(info)
        
        with open(file, "w") as f:
            f.write(str(metadata))
    
    
    def ReadLog(self):
        term = None
        message = None
        file = f"logs_node_{self.node_id}/log.txt"
        
        try:
            with Lock():
                with open(file, 'r') as file:
                    lines = file.readlines()
                    
                    for line in lines:
                        match = re.match(r'Term: (\d+)\. (.+)', line)
                        
                        if match:
                            term = int(match.group(1))
                            message = match.group(2)
                            
                            break
        except Exception as e:
            print(f"Error reading log file: {e}")
        
        return term, message
    
    
    def set_election_timeout(self, timeout=None):
        info = f"Term: {self.term}. Setting election timeout."
        print(info)
        LOG.info(info)
        
        if timeout:
            self.election_timeout = timeout
        else:
            self.election_timeout = time.time() + randint(self.election_period_ms,2*self.election_period_ms)/1000.0
    
    
    def Step_Down(self, term):
        info = f"Term: {self.term}. Stepping down from leader. Term: {term}."
        print(info)
        LOG.info(info)
        self.current_term = term
        self.state = 'FOLLOWER'
        self.voted_for = -1
        self.voted = False
        self.votes = []
        self.set_election_timeout()
    
    
    def On_Election_Timeout(self):
        info = f"Term: {self.term}. Election Timeout."
        print(info)
        LOG.info(info)
        
        while True:
            if time.time() > self.election_timeout and (self.state == 'FOLLOWER' or self.state == 'CANDIDATE'):
                print("Timeout....")
                self.set_election_timeout()
                self.Start_Election()
    
    
    def Start_Election(self):
        info = f"Term: {self.term}. Starting elections."
        print(info)
        LOG.info(info)
        self.state = 'CANDIDATE'
        self.voted_for = self.node_id
        self.current_term += 1
        self.voted = True
        self.votes.append(self.node_id)
        threads = []
        
        if len(self.nodes) == 1:
            self.state = 'LEADER'
            self.leader_id = self.node_id
            info = f"Votes: {self.votes}, Term: {self.current_term}"
            print(info)
            LOG.info(info)
            info = f"{self.node_id} became leader"
            print(info)
            LOG.info(info)
        
        for key, value in self.nodes.items():
            if key != self.node_id:
                t = Thread(target=self.request_vote, args=(key,))
                t.daemon = True
                t.start()
                threads += [t]
        
        for t in threads:
            t.join()
        
        return True
    
    
    def request_vote(self, node_id):
        last_index, last_term = self.commit_log.get_last_index_term()
        
        while True:
            info = f"Term: {self.term}. Requesting vote from {node_id}..."
            print(info)
            LOG.info(info)
            
            if self.state == 'CANDIDATE' and time.time() < self.election_timeout:
                msg = f"VOTE-REQ {self.node_id} {self.current_term} {last_term} {last_index}"
                port = self.nodes[node_id]["port"]
                channel = grpc.insecure_channel(port)
                stub = pb2_grpc.RaftServiceStub(channel)
                request = pb2.HandleRequestVoteRequest(
                    term = self.term,
                    candidate_id = self.node_id,
                    last_log_index = last_index,
                    last_log_term = last_term,
                    old_leader_lease_duration = 5
                )
                response = stub.RequestVote(request)
                print(response)
                
                if response:
                    vote_rep = re.match('^VOTE-REP ([0-9]+) ([0-9\-]+) ([0-9\-]+)$', response)
                    
                    if vote_rep:
                        server, curr_term, voted_for = vote_rep.groups()
                        server = int(server)
                        curr_term = int(curr_term)
                        voted_for = int(voted_for)
                        self.process_vote_reply(server, curr_term, voted_for)
                        
                        break
            else:
                break
    
    
    def process_vote_request(self, server, term, last_term, last_index):
        print(f"Term: {self.term}. Processing vote request from {server}.")
        
        if term > self.current_term:
            self.Step_Down(term)
        
        self_last_index, self_last_term = self.commit_log.get_last_index_term()
        
        if term == self.current_term and (self.voted_for == server or self.voted_for == -1) and (last_term > self_last_term or (last_term == self_last_term and last_index >= self_last_index)):
            self.voted_for = server
            self.state = 'FOLLOWER'
            self.set_election_timeout()
        
        return self.current_term, self.voted_for
    
    
    def process_vote_reply(self, server, term, voted_for):
        info = f"Term: {self.term}. Processing vote reply from {server} {term}..."
        print(info)
        LOG.info(info)
        
        if term > self.current_term:
            self.Step_Down(term)
        
        if term == self.current_term and self.state == 'CANDIDATE':
            if voted_for == self.node_id:
                self.votes.append(server)
            
            if len(self.votes) > len(self.nodes)/2.0:
                self.state = 'LEADER'
                self.leader_id = self.node_id
                info = f"Votes: {self.votes}, Term: {self.current_term}"
                print(info)
                LOG.info(info)
                info = f"{self.node_id} became leader"
                print(info)
                LOG.info(info)
    
    
    def leader_send_append_entries(self):
        info = f"Term: {self.term}. Sending append entries from leader..."
        print(info)
        LOG.info(info)
        
        while True:
            if self.state == 'LEADER':
                self.append_entries()
                last_index, _ = self.commit_log.get_last_index_term()
                self.commit_index = last_index
    
    
    def append_entries(self, log_entry=None, op=None):
        res = Queue()
        
        for key, values in self.nodes.items():
            if key != self.node_id:
                t = Thread(target=self.send_append_entries_request, args=(key, res, op, ))
                t.daemon = True
                t.start()
                t.join()
        
        if len(self.nodes) > 1:
            counts = 0
            start_time = time.time()
            
            while counts < (len(self.nodes) / 2.0) and time.time() - start_time < 5:
                res.get(timeout=1)
                counts += 1
                
                if counts >= (len(self.nodes)/2.0):
                    return
        else:
            return
    
    
    def send_append_entries_request(self, server, res=None, op=None):
        if not op:
            info = f"Term: {self.term}. Sending append entries to {server}..."
            print(info)
            LOG.info(info)
        else:
            info = f"Term: {self.term}. Sending heartbeat to {server}..."
            print(info)
            LOG.info(info)
        prev_idx = self.next_indices[int(server)]-1
        # log_slice = self.commit_log.read_logs_start_end(prev_idx)
        log_slice = []
        
        if prev_idx == -1:
            prev_term = 0
        else:
            if len(log_slice) > 0:
                prev_term = log_slice[0][0]
                log_slice = log_slice[1:] if len(log_slice) > 1 else []
            else:
                prev_term = 0
                log_slice = []
        
        msg = f"APPEND-REQ {self.node_id} {self.current_term} {prev_idx} {prev_term} {str(log_slice)} {self.commit_index}"
        
        while True:
            if self.state == 'LEADER':
                request = pb2.HandleAppendEntryRequest(
                    term = 1,
                    leader_id = 2,
                    prev_log_index = 3,
                    prev_log_term = 4,
                    log_entries = pb2.LogEntry(
                        log_index = 1,
                        term = self.term,
                        operation = ""
                    ),
                    leader_commit = 6,
                    lease_duration = 7
                )
                ip, port = self.conns[self.cluster_index][server]
                resp = utils.send_and_recv_no_retry(msg, ip, port, timeout = self.rpc_period_ms/1000.0)
                
                if resp:
                    append_rep = re.match('^APPEND-REP ([0-9]+) ([0-9\-]+) ([0-9]+) ([0-9\-]+)$', resp)
                    
                    if append_rep:
                        server, curr_term, flag, index = append_rep.groups()
                        server = int(server)
                        curr_term = int(curr_term)
                        flag = int(flag)
                        success = True if flag == 1 else False
                        index = int(index)
                        self.process_append_reply(server, curr_term, success, index)
                        
                        break
            else:
                break
        
        if res:
            res.put('ok')
    
    
    def process_append_requests(self, server, term, prev_idx, prev_term, logs, commit_index):
        info = f"Term: {self.term}. Processing append request from {server} {term}..."
        print(info)
        LOG.info(info)
        self.set_election_timeout()
        flag, index = 0, 0
        
        if term > self.current_term:
            self.Step_Down(term)
        
        if term == self.current_term:
            self.leader_id = server
            self_logs = self.commit_log.read_logs_start_end(prev_idx, prev_idx) if prev_idx != -1 else []
            success = prev_idx == - 1 or (len(self_logs) > 0 and self_logs[0][0] == prev_term)
            
            if success:
                last_index, last_term = self.commit_log.get_last_index_term()
                
                if len(logs) > 0 and last_term == logs[-1][0] and last_index == self.commit_index:
                    index = self.commit_index
                else:
                    index = self.store_entries(prev_idx, logs)
            
            flag = 1 if success else 0
        
        return f"APPEND-REP {self.node_id} {self.current_term} {flag} {index}"
    
    
    def process_append_reply(self, server, term, success, index):
        info = f"Term: {self.term}. Processing append reply from {server} {term}..."
        print(info)
        LOG.info(info)
        
        if term > self.current_term:
            self.Step_Down(term)
        
        if self.state == 'LEADER' and term == self.current_term:
            if success:
                self.next_indices[int(server)] = index+1
            else:
                self.next_indices[int(server)] = max(0, self.next_indices[int(server)]-1)
                self.send_append_entries_request(server)
    
    
    def store_entries(self, prev_idx, leader_logs):
        commands = [f"{leader_logs[i][1]}" for i in range(len(leader_logs))]
        last_index, _ = self.commit_log.log_replace(self.current_term, commands, prev_idx+1)
        self.commit_index = last_index
        
        for command in commands:
            self.update_state_machine(command)
        
        return last_index
    
    
    def update_state_machine(self, command):
        set_ht = re.match('^SET ([^\s]+) ([^\s]+) ([0-9]+)$', command)
        
        if set_ht:
            key, value, req_id = set_ht.groups()
            req_id = int(req_id)
            self.ht.set(key=key, value=value, req_id=req_id)
    
    
    def ReplicateLog(self, key, value, req_id):
        last_index, _ = self.commit_log.get_last_index_term()
        log_entry = f"SET {key} {value} {req_id}"
        self.append_entries(log_entry)
    
    
    def handle_commands(self, msg):
        set_ht = re.match('^SET ([^\s]+) ([^\s]+) ([0-9]+)$', msg)
        get_ht = re.match('^GET ([^\s]+) ([0-9]+)$', msg)
        # vote_req = re.match('^VOTE-REQ ([0-9]+) ([0-9\-]+) ([0-9\-]+) ([0-9\-]+)$', msg)
        # append_req = re.match('^APPEND-REQ ([0-9]+) ([0-9\-]+) ([0-9\-]+) ([0-9\-]+) (\[.*?\]) ([0-9\-]+)$', msg)
        
        if set_ht:
            output = "ko"
            
            try:
                key, value, req_id = set_ht.groups()
                req_id = int(req_id)
                self.ReplicateLog(key, value, req_id)
                node = mmh3.hash(key, signed=False) % len(self.nodes)
                
                if self.cluster_index == node:
                    while True:
                        if self.state == 'LEADER':
                            last_index, _ = self.commit_log.log(self.current_term, msg)
                            
                            while True:
                                if last_index == self.commit_index:
                                    break
                            
                            self.ht.set(key=key, value=value, req_id=req_id)
                            output = "ok"
                            
                            break
                        else:
                            if self.leader_id != -1 and self.leader_id != self.node_id:
                                output = pb2.HandleServeClientResponse(
                                    data = msg,
                                    leader_id = self.leader_id,
                                    status = True
                                )
                                
                                if output is not None:
                                    break
                            else:
                                output = 'ko'
                                
                                break
                else:
                    output = utils.send_and_recv(msg, self.conns[node][0][0], self.conns[node][0][1])
                    
                    if output is None:
                        output = "ko"
            except Exception as e:
                traceback.print_exc(limit=1000)
        elif get_ht:
            output = "ko"
            
            try:
                key, _ = get_ht.groups()
                node = mmh3.hash(key, signed=False) % len(self.nodes)
                
                if self.cluster_index == node:
                    while True:
                        if self.state == 'LEADER':
                            output = self.ht.get_value(key=key)
                            if output:
                                output = str(output)
                            else:
                                output = 'Error: Non existent key'
                            break
                        else:
                            if self.leader_id != -1 and self.leader_id != self.node_id:
                                output = utils.send_and_recv_no_retry(msg, self.conns[node][self.leader_id][0], self.conns[node][self.leader_id][1], timeout=self.rpc_period_ms)
                                
                                if output is not None:
                                    break
                            else:
                                output = 'ko'
                                
                                break
                else:
                    output = utils.send_and_recv(msg, self.conns[node][0][0], self.conns[node][0][1])
                    
                    if output is None:
                        output = "ko"
            except Exception as e:
                traceback.print_exc(limit=1000)
        else:
            print("Hello1 - " + msg + " - Hello2")
            output = "Error: Invalid command"
        
        return output
    
    
    def StartNode(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        pb2_grpc.add_RaftServiceServicer_to_server(self, server)
        server.add_insecure_port(self.port)
        self.GetLeader()
        server.start()
        server.wait_for_termination()
    
    
    def GetLeader(self):
        global LOG
        # last_index, last_term = self.commit_log.get_last_index_term()
        
        if self.leader_id:
            return self.leader_id
        else:
            try:
                if not self.voted:
                    self.Start_Election()
                    # request, msg = self.is_vote_request_in_channel(self.node_id)
                    
                    # if request:
                    #     channel = grpc.insecure_channel(self.port)
                    #     stub = pb2_grpc.RaftServiceStub(channel)
                    #     self.ProcessRequest(request)
                    # else:
                    #     self.Start_Election()
            except Exception as e:
                error = f"Term: {self.term}. Error checking for existing vote request: {e}"
                print(error)
                LOG.error(error)
    
    
    def RequestVote(self, request, context):
        term = request.term
        candidate_id = request.candidate_id
        last_term = request.last_log_term
        last_index = request.last_log_index
        old_leader_lease_duration = request.old_leader_lease_duration
        
        try:
            term, voted_for = self.process_vote_request(candidate_id, term, last_term, last_index)
            response = pb2.HandleRequestVoteResponse(
                term = self.term,
                vote_granted = (voted_for == candidate_id),
                longest_old_leader_lease = 5
            )
        except Exception as e:
            response = pb2.HandleRequestVoteResponse(
                term = self.term,
                vote_granted = False,
                longest_old_leader_lease = 5
            )
            traceback.print_exc(limit=1000)
    
    
    def AppendEntry(self, request, context):
        node_id = request.leader_id
        term = request.term
        prev_idx = request.prev_log_index
        prev_term = request.prev_log_term
        logs = request.log_entries
        commit_index = request.leader_commit
        lease_duration = request.lease_duration
        
        try:
            output = self.process_append_requests(node_id, term, prev_idx, prev_term, logs, commit_index)
        except Exception as e:
            response = pb2.HandleAppendEntryResponse(
                term = self.term,
                status = False
            )
            traceback.print_exc(limit=1000)
        
        return response
    
    
    def ServeClient(self, request, context):
        msg = request.request
        
        try:
            output = self.handle_commands(msg)
        except Exception as e:
            response = pb2.HandleServeClientResponse(
                data = "",
                leader_id = self.leader_id,
                status = False
            )
            traceback.print_exc(limit=1000)
        
        return response


def process_arguments():
    if len(sys.argv) != 3:  
        print("Usage: python file.py node_id array")
        sys.exit(1)
    
    node_id = sys.argv[1]
    array_str = sys.argv[2] 
    
    try:
        array = ast.literal_eval(array_str)
        
        if not isinstance(array, dict):
            raise ValueError
    except (ValueError, SyntaxError):
        print("Error: Invalid array format. Please provide a valid Python list.")
        sys.exit(1)
    
    node_id = str(int(node_id)).zfill(3)
    temp = {}
    
    for key, value in array.items():
        t1 = str(int(key)).zfill(3)
        t2 = str(value["port"])
        t3 = str(value["address"])
        temp[t1] = {
            'port' : t2,
            'address' : t3
        }
    
    return node_id, temp


def main():
    global BASE_DIR, LOG, MyTHREAD
    NODE_ID, NODES = process_arguments()
    PORT = NODES[NODE_ID]["port"]
    ADDRESS = NODES[NODE_ID]["address"]
    BASE_DIR = Path.cwd()
    node_dir = BASE_DIR / f"logs_node_{NODE_ID}"
    node_dir.mkdir(parents = True, exist_ok = True)
    logging.basicConfig(filename = node_dir / "log.txt", format='%(asctime)s %(message)s', filemode = 'w')
    LOG = logging.getLogger()
    LOG.setLevel(logging.DEBUG)
    MyTHREAD = threading.Thread(target=RaftNode, args=(NODE_ID, NODES, PORT, ADDRESS))
    MyTHREAD.daemon = True
    MyTHREAD.start()
    MyTHREAD.join()


if __name__ == "__main__":
    main()