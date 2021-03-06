import time
import Queue
import threading
import sys
import SocketServer
import BaseHTTPServer
import urllib2
import random
import collections


class Event(object):
    def __init__(self, command, args):
        self.command = command
        self.args = args
        self.is_handled = False
        self.response = None

    def set_response(self, response):
        self.response = response

    def mark_handled(self):
        if self.is_handled:
            raise RuntimeError("Already handled")

        self.is_handled = True


class RaftNode(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):

        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.end_headers()

        # Make event object from path
        data = [i for i in self.path.split("/") if i]
        command = data[0]
        args = data[1:]
        event = Event(command, args)

        RaftNode.queue.put(event)

        while True:
            time.sleep(0.05)
            if event.is_handled:
                break

        # Send the html message
        self.wfile.write(event.response if event.response is not None else "OK")


def server_main(queue, port):
    # global variable, just to share queue with RaftNode
    RaftNode.queue = queue
    server = SocketServer.TCPServer(("", port), RaftNode)
    server.serve_forever()


def election_timeout(queue):
    queue.put(Event("ElectionTimeout", []))


FOLLOWER = 1
LEADER = 2
CANDIDATE = 3


def fetch_on_thread(address, queue=None):
    def target():
        response = urllib2.urlopen(address).read()
        if response and queue:
            data = response.split()
            queue.put(Event(data[0], data[1:]))

    thread = threading.Thread(target=target)
    thread.start()


def send_vote_request(node, current_term, current_node, queue):
    address = "http://localhost:%s/VoteRequest/%s/%s" % (
        node, current_term, current_node)
    fetch_on_thread(address, queue)


def send_vote(node, current_term):
    address = "http://localhost:%s/VoteGot/%s" % (node, current_term)
    fetch_on_thread(address)


def send_append_value(node, current_term, prev_log_index, prev_log_term, command):
    address = "http://localhost:%s/AppendValue/%s/%s/%s/%s" % (node, current_term, prev_log_index, prev_log_term, command)
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
        queue.put(Event("Heartbeat", []))


LogEntry = collections.namedtuple("LogEntry", ["command", "term"])


def worker_main(queue, nodes):
    state = FOLLOWER
    voted_for = {}
    num_votes = 0
    current_term = 0
    log = [LogEntry(0,0)]
    next_index = []

    heartbeat_timer = threading.Thread(target=heartbeat_main, args=(queue, ))
    heartbeat_timer.start()

    timer_to_election = threading.Timer(5, election_timeout, args=(queue, ))
    timer_to_election.start()
    while True:
        event = queue.get()
        command = event.command
        args = event.args
        if command != "Heartbeat" or state == LEADER:
            print nodes[0], state, "got", command, args, "log", log

        if state in [FOLLOWER, CANDIDATE]:
            if command == "AppendValue":
                term, prev_log_index, prev_log_term = [int(x) for x in args[:3]]
                command = args[3]
                timer_to_election = reset_timer(timer_to_election, queue)
                if len(log) >= prev_log_index or log[prev_log_index].term != prev_log_term:
                    event.set_response("AppendValueResponse %s False" % current_term)
                else:
                    if len(log) > prev_log_index:
                        if log[prev_log_index+1].term != term:
                            log = log[:prev_log_index+1]
                    print "Added new log", command
                    log.append(LogEntry(command, term))
                    event.set_response("AppendValueResponse %s True" % current_term)

            elif command == "ElectionTimeout":
                current_term += 1
                num_votes = 1
                voted_for[current_term] = nodes[0]
                state = CANDIDATE
                for node in nodes[1:]:
                    send_vote_request(node, current_term, nodes[0], queue)
                timer_to_election = reset_timer(timer_to_election, queue)


            elif command == "VoteGot":
                if int(args[0]) == current_term and args[1] == "True":
                    num_votes += 1

                if num_votes > len(nodes) // 2:
                    state = LEADER
                    # TODO: should this take into account if committed / applied to state machine?
                    last_log_index = len(log)-1
                    next_index = [last_log_index for n in nodes[1:]]
                    timer_to_election.cancel()

            elif command == "VoteRequest":
                term = int(args[0])
                if term >= current_term and not voted_for.get(term):
                    current_term = term
                    candidate = int(args[1])

                    assert candidate in nodes
                    event.set_response("VoteGot %s True" % term)
                    voted_for[term] = candidate
                    timer_to_election = reset_timer(timer_to_election, queue)
                else:
                    event.set_response("VoteGot %s False" % term)

        elif state == LEADER:

            if command == "Heartbeat":
                for i, node in enumerate(nodes[1:]):
                    prev_log_index = next_index[i]
                    prev_log_term = log[prev_log_index].term
                    send_append_value(node, current_term, prev_log_index, prev_log_term, "None")

            elif command == "ClientAppendValue":
                command = event.args[0]
                log.append(LogEntry(current_term, command))
                for node in nodes[1:]:
                    prev_log_index = next_index[i]
                    prev_log_term = log[prev_log_index].term
                    send_append_value(node, current_term, prev_log_index, prev_log_term, command)

            elif command == "AppendValueResponse":
                follower_current_term, success = args
                print args
                # if success == "False":
                    # assert


        event.mark_handled()


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
