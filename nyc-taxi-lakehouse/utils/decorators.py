def run_before(method_name):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            pre_method = getattr(self, method_name)
            pre_method()
            return func(self, *args, **kwargs)
        return wrapper
    return decorator