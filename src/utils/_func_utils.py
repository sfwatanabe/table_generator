
def func_timer(some_function):
    """
    Times the function execution and returns a tuple with the
    result and the time taken.
    """
    from time import perf_counter

    def wrapper(*args, **kwargs):
        t1 = perf_counter()
        result = some_function(*args, **kwargs)
        end = perf_counter()-t1
        return result, end
    return wrapper
