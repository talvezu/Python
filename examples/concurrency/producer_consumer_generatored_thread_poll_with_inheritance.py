#!/usr/bin/env python
#
# Written by Nir I Levy
# GitHub: https://github.com/talvezu
# Email: talvezu@walla.com
#
# This code has been released under the terms of the Apache-2.0 license
# http://opensource.org/licenses/Apache-2.0
#


from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict, defaultdict
import threading
import time
import uuid
import queue

'''
Base class for tasks which differ in the flow of it's state machine
A class is task that has {square triple quad done} states
B class is task that has {add substract done} states
derieved classes should implement the following methods
next    - state machine flow
task_{} - implementation of the 'generator-like' step
'''
class Base:
    i = {}
    status = {}
    def __init__(self, i, done_queue):
        self.i = i
        self.status = "init"
        self.done_queue = done_queue
        print("new object")
        return

    def next(self):
        print("Base:next called!!!")
        return

    def done(self, fn):
        message = {}
        print("done task {} for id {}".format(self.status, self.i)) 
        if fn.cancelled():
            print('{}: canceled'.format(fn.arg))
            code = "cancelled"
            message = "cancelled due to user request"
        elif fn.done():
            error = fn.exception()
            if error:
                print('{}: error returned: {}'.format(fn.arg, error))
                code = "error"
                message = "error received:" + error
            else:
                result = fn.result()
                print('{}: value returned: {}'.format(fn.arg, result))
                code = "OK"
                message = result
        try:
            #todo remove timeout
            self.done_queue.put((self, code, message))
            print("id:{} done callback for stage {}".format(self.i, self.status))
        except queue.Empty:
            print("task object queue for indicating done task full")
    


class A(Base):

    def task_square(self):
        time.sleep(3)
        self.status = "square"
        return self

    def task_triple(self):
        time.sleep(5)
        self.status = "triple"
        return self

    def task_quad(self):
        time.sleep(7)
        self.status = "quad"
        return self

    def next(self):
        if self.status == "init":
            return self.task_square()
        if self.status == "square":
            return self.task_triple()
        if self.status == "triple":
            return self.task_quad()
        if self.status == "quad":
            self.status = "done" 
            return self
        if self.status == "done":
            return self
        print ("{} reached illegel state!".format(self.i))
        return self


class B(Base):
    def task_add(self):
        time.sleep(2)
        self.status = "add"
        return self

    def task_substract(self):
        time.sleep(20)
        self.status = "substract"
        return self

    def next(self):
        print("next status from {} for id {}".format(self.status, self.i))
        if self.status == "init":
            return self.task_add()
        if self.status == "add":
            return self.task_substract()
        if self.status == "substract":
            self.status = "done"
            return self
        if self.status == "done":
            return self
        print ("{} reached illegel state!".format(self.i))
        return self
    

'''
holds dictionary of Futures
every loop queue (new_tasks_queue) is checked and new tasks associated with uid granted a Future.

later on every future is checked whereas done(), and result is saved in an orderedDict
result are the iteration of the task returned by next.

TODO:
1)change the done() to the callback for iteration of next task.
3)add clear of the operation
     
'''
class context_manager(threading.Thread):        
    Futures=dict()
    completed_futures = []
    results=defaultdict(OrderedDict)
    pool={}
    new_tasks_queue={}
    done_tasks_queue={}
    def __init__(self, threads_count, name, threadID, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        squares=list(range(1,threads_count))
        self.pool = ThreadPoolExecutor(threads_count)
        self.new_tasks_queue=q
        self.done_tasks_queue=queue.Queue()
     
    def run(self):
        while True:
            more_task_requests = True
            task = {}
            while more_task_requests:
                try:
                    task = self.new_tasks_queue.get(block=False)
                    uid = task[0]
                    #conv_obj = task[1]+'('+str(uid)+','+ str(self.done_tasks_queue)+')'
                    conv_obj = task[1]+'('+str(uid)+','+ "self.done_tasks_queue"+')'
                    print ("conv object is : {}".format(conv_obj))
                    obj = eval(conv_obj)
                    #obj.next()                    
                    self.Futures[uid] = self.pool.submit(obj.next)                
                    self.Futures[uid].arg=obj.i
                    self.Futures[uid].add_done_callback(obj.done)
                except queue.Empty:
                    more_task_requests = False
                    
            done_task_requests = True    
            while done_task_requests:
                try:
                    task = self.done_tasks_queue.get(block=False)
                    obj = task[0]
                    code = task[1]
                    message = task[2]
                    if code == "cancelled":
                        print ("object: {}, {}".format(obj.i, message))
                    if code == "error":
                        print ("object: {}, {}".format(obj.i, message))
                    if code == "OK":
                        print ("object: {}, completed stage {}".format(obj.i, obj.status))
                        if obj.status == "done":
                            self.results[obj.i][obj.status] = "done"
                            print("finished obj calculating: {}".format(obj.i))
                        else:
                            print("reinserting obj {} for next task: {}".format(obj.i, obj.status))
                            self.Futures[obj.i] = self.pool.submit(obj.next)
                            self.Futures[obj.i].arg=obj.i
                            self.Futures[obj.i].add_done_callback(obj.done)
                except queue.Empty:
                    done_task_requests = False

            time.sleep(1)
    def print_res(self):
        for item in self.results:
            print ("for: {}, results: {}".format(item, self.results[item]))
            #print
            #for key in self.result.item:
            #    print ("{}".format(self.result))
            

'''
singletone used as design pattern to ensure only one context handling the messages
'''
class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

'''
context
object, responsible for running the thread pool handling the messages
'''
class context(metaclass=Singleton):
    def __init__(self, pool_size, q):
        self.cm = context_manager(pool_size, "context manager", 3031, q)

    def run(self):
        self.cm.start()
        self.cm.join()

    def get_res(self):
        self.cm.print_res()


'''
entry point for the thread responsible running the context
'''
def handle_ongoing_requests(pool_size, q):
    cm = context(pool_size,q)
    cm.run()

message_queue=queue.Queue()
thread = threading.Thread(target=handle_ongoing_requests, args=(10, message_queue))
thread.start()


'''
entry point for dispatching task
read objects from file and inserting then the the message_queue later read and handle by the context manager thread pool
'''
def dispatch_task( threadName, delay, q):
    uid_list = []
    count = 0
    while count < 3:
        time.sleep(delay)
        count += 1
        print ("%s: %s" % ( threadName, time.ctime(time.time()) ))

    moves = map(str.rstrip, open("tasks.txt").readlines())
    for move in moves:
        time.sleep(1)
        print (move)
        try:
            #todo remove timeout
            uid = uuid.uuid4()
            uid_list.append(uid)
            obj = move
            print("dispatching obj")
            q.put((uid.int, obj))
        except queue.Full:
            print("dispatch queue full")
 
        
   

dispatcher = threading.Thread(target=dispatch_task, args=("dispatcher_thread",2 ,message_queue) )
dispatcher.start()


