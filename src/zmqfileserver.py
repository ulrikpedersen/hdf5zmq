'''
Created on 17 Aug 2015

Copied from zguide: http://zguide.zeromq.org/py:all#toc211

'''

# File Transfer model #3
#
# In which the client requests each chunk individually, using
# command pipelining to give us a credit-based flow control.

import os
from threading import Thread
import binascii

import zmq
#import zmq.PAIR, zmq.DEALER, zmq.ROUTER, zmq.ETERM

CHUNK_SIZE = 25000
PIPELINE = 100
        
def zpipe(ctx):
    """build inproc pipe for talking to threads
    mimic pipe used in czmq zthread_fork.
    Returns a pair of PAIRs connected via inproc
    """
    a = ctx.socket(zmq.PAIR)
    b = ctx.socket(zmq.PAIR)
    a.linger = b.linger = 0
    a.hwm = b.hwm = 1
    iface = "inproc://%s" % binascii.hexlify(os.urandom(8))
    a.bind(iface)
    b.connect(iface)
    return a,b

def client_thread(ctx, pipe):
    dealer = ctx.socket(zmq.DEALER)
    dealer.hwm = PIPELINE
    dealer.connect("tcp://127.0.0.1:6000")

    total = 0       # Total bytes received
    chunks = 0      # Total chunks received
    credit = PIPELINE
    fetchcount = 0

    while True:
        while credit:
            # ask for next chunk
            print "client: fetch part: %i" % total 
            dealer.send_multipart([
                b"fetch",
                b"%i" % total,
                b"%i" % CHUNK_SIZE
            ])
            credit -= 1
            fetchcount += 1

        try:
            print "client: recv..."
            chunk = dealer.recv()
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                return   # shutting down, quit
            else:
                raise

        size = len(chunk)
        print "client: got it (size=%i)!"%size
        chunks += 1
        credit += 1
        total += size
        if size < CHUNK_SIZE:
            break   # Last chunk received; exit

    print ("%i chunks received (%i), %.3f MB fetchcount=%i" % 
           (chunks, credit, total/(1024. *1024.), fetchcount))
    pipe.send(b"OK")

# The server thread waits for a chunk request from a client,
# reads that chunk and sends it back to the client:

def server_thread(ctx):
    f = open("testdata.h5", "r")

    router = ctx.socket(zmq.ROUTER)

    router.bind("tcp://*:6000")
    # We have two parts per message so HWM is PIPELINE * 2
    router.hwm = PIPELINE * 2 

    while True:
        # First frame in each message is the sender identity
        # Second frame is "fetch" command
        try:
            print "server: recv..."
            msg = router.recv_multipart()
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                return   # shutting down, quit
            else:
                raise

        print "server: got fetch"
        identity, command, offset_str, chunksz_str = msg

        assert command == b"fetch"

        offset = int(offset_str)
        chunksz = int(chunksz_str)

        # Read chunk of data from file
        f.seek(offset, os.SEEK_SET)
        data = f.read(chunksz)
        if len(data) < chunksz:
            print "server: LAST chunk read!!! %i" % len(data)
        if len(data)==0:
            print "server: READ TO END OF FILE. QUITTING!"
            return

        # Send resulting chunk to client
        print "server: sending data..."
        router.send_multipart([identity, data], copy=False)

# The main task is just the same as in the first model.

def main():

    # Start child threads
    ctx = zmq.Context()
    a,b = zpipe(ctx)

    client = Thread(target=client_thread, args=(ctx, b))
    server = Thread(target=server_thread, args=(ctx,))
    client.start()
    server.start()

    # loop until client tells us it's done
    try:
        print a.recv()
    except KeyboardInterrupt:
        pass
    del a,b
    ctx.term()

if __name__ == '__main__':
    main()
    