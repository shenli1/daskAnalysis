import timeit

def time(func):
    def clocked(*args, **kwargs):
        start = timeit.default_timer()
        res = func(*args, **kwargs)
        run_time = timeit.default_timer() - start
        func_name = func.__name__
        #arg_str = ', '.join(repr(arg) for arg in args)
        #print('调用>>>%s(%s)   返回值>>>%r   耗时>>>%0.8fs' % (func_name, arg_str, res, run_time))
        print('调用>>>%s  耗时>>>%0.8fs \n返回值>>>%r' % (func_name, run_time,res))
        return res

    return clocked