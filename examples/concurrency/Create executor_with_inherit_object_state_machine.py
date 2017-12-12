from concurrent.futures import ThreadPoolExecutor
#from time import sleep
import time

from collections import OrderedDict, defaultdict
import threading
import uuid

#for row in squares:
#    print("[{}]".format(row))
class Base:
    i = {}
    status = {}
    def __init__(self, i):
        self.i = i
        self.status = "init"
        return

    def next(self):
        print("Base:next called!!!")
        return

    def done(self, fn):
        if fn.cancelled():
            print('{}: canceled'.format(fn.arg))
        elif fn.done():
            error = fn.exception()
            if error:
                print('{}: error returned: {}'.format(
                    fn.arg, error))
            else:
                result = fn.result()
                print('{}: value returned: {}'.format(
                    fn.arg, result))



class A(Base):

    def task_square(self):
        time.sleep(3)
        self.status = "square"
        return self, ([self.i, self.i**2])

    def task_triple(self):
        time.sleep(5)
        self.status = "triple"
        return self, ([self.i, self.i**3])

    def task_quad(self):
        time.sleep(7)
        self.status = "quad"
        return self, ([self.i, self.i**4])

    def next(self):
        if self.status == "init":
            return self.task_square()
        if self.status == "square":
            return self.task_triple()
        if self.status == "triple":
            return self.task_quad()
        if self.status == "quad":
            return

class B(Base):
    def task_add(self):
       time.sleep(2)
       self.status = "add"
       return self, ([self.i, self.i+10])

    def task_substract(self):
       time.sleep(20)
       self.status = "substract"
       return self, ([self.i, self.i-10])

    def next(self):
       if self.status == "init":
           return self.task_add()
       if self.status == "add":
           return self.task_substract()
       if self.status == "substract":
           return self, ([0, 0])


class context_manager(threading.Thread):
    Futures=dict()
    completed_futures = []
    results=defaultdict(OrderedDict)
    pool={}
    task_queue=dict()
    lock=threading.Lock()
    def __init__(self, threads_count, name, threadID):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        squares=list(range(1,threads_count))
        self.pool = ThreadPoolExecutor(threads_count)
        for square in squares:
            #self.Futures[square] = self.pool.submit(A.next, A(square))
            n_task = B(square)
            self.Futures[square] = self.pool.submit(n_task.next)
            self.Futures[square].arg=square
            self.Futures[square].add_done_callback(n_task.done)
            print ("inserting {} to array".format(square))

    def add_task(self, obj):
        uid = uuid.uuid4()
        with self.lock:
            task_queue([uid, obj])

    def run(self):
        while True:
            for count, Future in self.Futures.items():
                if Future.done():
                    res = Future.result()
                    self.completed_futures.append(count)
                    obj = res[0]
                    self.results[res[0].i][obj.status] = res[1][1]
                    print ("{} done".format(count))

            if self.completed_futures:
                for item in self.completed_futures:
                    res = self.Futures[item].result()
                    obj = res[0]
                    print ("removing {} from Futures".format(item))
                    del self.Futures[item]
                    #if obj.status == "quad":
                    if obj.status == "substract":
                        print("finished obj calculating: {}".format(obj.i))
                        #self.Futures[obj.i] = self.pool.submit(B.next, B(obj.i))
                    else:
                        print("reinserting obj {} for next task: {}".format(obj.i, obj.status))
                        #self.Futures[obj.i] = self.pool.submit(A.next, obj )
                        self.Futures[obj.i] = self.pool.submit(obj.next)
                        self.Futures[obj.i].arg=obj.i
                        self.Futures[obj.i].add_done_callback(obj.done)
            del self.completed_futures[:]
            if not self.Futures:
                print ("no more futures, exit")
                break

    def print_res(self):
        for item in self.results:
            print ("for: {}, results: {}".format(item, self.results[item]))
            #print
            #for key in self.result.item:
            #    print ("{}".format(self.result))



class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class context(metaclass=Singleton):
    def __init__(self):
        self.cm = context_manager(10, "context manager", 3031)

    def run(self):
        self.cm.start()
        self.cm.join()

    def get_res(self):
        self.cm.print_res()

c=context()
c.run()
c.get_res()

print("X")


