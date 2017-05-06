import time
import Queue
import threading
import sys
import SocketServer
import BaseHTTPServer
import urllib2
import random


class RaftNode(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        RaftNode.queue.put(self.path)

        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.end_headers()
        # Send the html message
        self.wfile.write("Hello World !")


def server_main(queue, port):
    # global variable, just to share queue with RaftNode
    RaftNode.queue = queue
    server = SocketServer.TCPServer(("", port), RaftNode)
    server.serve_forever()


def election_timeout(queue):
    queue.put("ElectionTimeout")


FOLLOWER = 1
LEADER = 2
CANDIDATE = 3


def fetch_on_thread(address):
    def target():
        print "Fetching", address
        urllib2.urlopen(address)
        print "Done...", address

    thread = threading.Thread(target=target)
    thread.start()


def send_vote_request(node, current_term, current_node):
    address = "http://localhost:%s/VoteRequest/%s/%s" % (
        node, current_term, current_node)
    fetch_on_thread(address)


def send_vote(node, current_term):
    address = "http://localhost:%s/VoteGot/%s" % (node, current_term)
    fetch_on_thread(address)


def send_append_value(node, current_term):
    address = "http://localhost:%s/AppendValue/%s" % (node, current_term)
    fetch_on_thread(address)


def reset_timer(timer, queue):
    timer.cancel()
    new_timer = threading.Timer(
        5 + random.random() * 3.0,
        election_timeout,
        args=(queue, ))
    new_timer.start()

    return new_timer


def heartbeat_main(queue):
    while True:
        time.sleep(1)
        queue.put("Heartbeat")


def worker_main(queue, nodes):
    state = FOLLOWER
    voted_for = {}
    num_votes = 0
    current_term = 0

    heartbeat_timer = threading.Thread(target=heartbeat_main, args=(queue, ))
    heartbeat_timer.start()

    timer_to_election = threading.Timer(5, election_timeout, args=(queue, ))
    timer_to_election.start()
    while True:
        data = [i for i in queue.get().split("/") if i]
        command = data[0]
        args = data[1:]
        if command != "Heartbeat":
            print "Got", command, args

        if state in [FOLLOWER, CANDIDATE]:
            if command == "AppendValue":
                timer_to_election = reset_timer(timer_to_election, queue)

            elif command == "ElectionTimeout":
                current_term += 1
                num_votes = 1
                voted_for[current_term] = nodes[0]
                state = CANDIDATE
                for node in nodes[1:]:
                    send_vote_request(node, current_term, nodes[0])
                timer_to_election = reset_timer(timer_to_election, queue)


            elif command == "VoteGot":
                if int(args[0]) == current_term:
                    num_votes += 1

                if num_votes > len(nodes) // 2:
                    state = LEADER
                    timer_to_election.cancel()

            elif command == "VoteRequest":
                term = int(args[0])
                if term < current_term:
                    continue
                current_term = term
                candidate = int(args[1])
                if not voted_for.get(term):
                    assert candidate in nodes
                    send_vote(candidate, term)
                    voted_for[term] = candidate
                    timer_to_election = reset_timer(timer_to_election, queue)

        elif state == LEADER:
            if command == "Heartbeat":
                for node in nodes[1:]:
                   send_append_value(node, current_term)


def main(nodes):
    queue = Queue.Queue()

    server_thread = threading.Thread(
        target=server_main,
        args=(queue, nodes[0]))
    server_thread.daemon = True
    server_thread.start()

    worker_thread = threading.Thread(
        target=worker_main,
        args=(queue, nodes))
    worker_thread.daemon = True
    worker_thread.start()

    while True:
        time.sleep(100)

if __name__ == "__main__":
    nodes = [int(i) for i in sys.argv[1:]]
    main(nodes)
