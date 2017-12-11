from concurrent.futures import ThreadPoolExecutor
from time import sleep
from collections import defaultdict


#for row in squares:
#    print("[{}]".format(row))

class A:
    i = {}
    status = {}
    def __init__(self, i):
        self.i = i
        self.status = "init"
        return

    def task_square(self):
        sleep(3)
        self.status = "square"
        return self, ([self.i, self.i**2])

    def task_triple(self):
        sleep(5)
        self.status = "triple"
        return self, ([self.i, self.i**3])

    def task_quad(self):
        sleep(7)
        self.status = "quad"
        return self, ([self.i, self.i**4])

    def task_done(self):
        self.status = "done"
        return self, ([0, 0])

    def next(self):
        if self.status == "init":
            return self.task_square()
        if self.status == "square":
            return self.task_triple()
        if self.status == "triple":
            return self.task_quad()
        if self.status == "quad":
            return self.task_done()
            

class context_manager:
    Futures=dict()
    completed_futures = []
    results=defaultdict(dict)
    pool={}
    def __init__(self, threads_count):
        squares=list(range(1,threads_count))
        self.pool = ThreadPoolExecutor(threads_count)
        for square in squares:
            self.Futures[square] = self.pool.submit(A.next, A(square))
            print ("inserting {} to array".format(square))

    def main_routine(self):
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
                    if obj.status == "done":
                        print("finished obj calculating: {}".format(obj.i))
                        self.results[res[0].i][obj.status] = res[1][1]
                    else:
                        print("reinserting obj {} for next task: {}".format(obj.i, obj.status))
                        self.Futures[obj.i] = self.pool.submit(A.next, obj )
            del self.completed_futures[:]
            if not self.Futures:
                print ("no more futures, exit")
                break


cm = context_manager(10)
cm.main_routine()
