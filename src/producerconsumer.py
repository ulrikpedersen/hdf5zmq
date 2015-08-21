'''
Created on 21 Aug 2015

@author: up45
'''

import os, logging
from threading import Thread
import binascii

import zmq
import numpy as np
#import zmq.PAIR, zmq.DEALER, zmq.ROUTER, zmq.ETERM

logging.basicConfig(level=logging.DEBUG)

PIPELINE = 2



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

def producer(ctx):
    '''The producer function generates numpy arrays
    
    The arrays are sent out on a ROUTER socket as per request from the consumer.
    The function returns (None) when all available arrays has been sent.
    '''
    log = logging.getLogger("producer")
    router = ctx.socket(zmq.ROUTER)
    socket_name = "tcp://*:6000"
    log.info("Binding socket name: %s", socket_name)
    router.bind(socket_name)
    # We have two parts per message so HWM is PIPELINE * 2
    router.hwm = PIPELINE * 2 
    log.info("Router HWM: %d", router.hwm)
    
    num_arrays = 5
    array_counter = 0
    log.info("Number of arrays to send: %d", num_arrays)
    
    while True:
        # First frame in each message is the sender identity
        # Second frame is "fetch" command
        try:
            log.debug("receiving...")
            msg = router.recv_multipart()
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                log.warning("Shutting down. zmq.ETERM")
                return   # shutting down, quit
            else:
                raise

        log.debug( "got msg" )
        identity, command = msg
        log.debug("identity: %s  cmd: %s", identity, command)
        assert command == b"fetch"
        
        
        data = b"blah"
        log.debug("Sending data: %s", data)
        router.send_multipart([identity, data], copy=False)
        array_counter +=1
        if array_counter > num_arrays:
            break
    log.info("Shutting down.")


def consumer(ctx, pipe):
    '''The consumer requests and receives numpy arrays and write them to file
    
    The arrays are appended to a HDF5 dataset. New arrays are being requested in
    prefetch manner so that data is meant to be always available for writing.
    '''
    log = logging.getLogger("consumer")
    dealer = ctx.socket(zmq.DEALER)
    
    socket_name = "tcp://127.0.0.1:6000"
    log.info("Connecting to socket name: %s", socket_name)
    dealer.connect(socket_name)
    dealer.hwm = PIPELINE
    log.debug("Dealer HWM: %d", dealer.hwm) 

    credit = PIPELINE
    while True:
        while credit:
            log.debug("Request: gimme more")
            dealer.send_multipart([b"fetch"])
            credit -= 1
        
        log.debug("Receive...")
        try:
            raw_data = dealer.recv()
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                log.warning("Shutting down. zmq.ETERM")
                return
            else:
                raise
        log.debug("Received: %d", len(raw_data))
        log.debug(raw_data)
        
    log.info("Consumer finished...")
    pipe.send(b"OK")

def main():

    # Start child threads
    ctx = zmq.Context()
    a,b = zpipe(ctx)

    client = Thread(target=consumer, args=(ctx, b))
    server = Thread(target=producer, args=(ctx,))
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