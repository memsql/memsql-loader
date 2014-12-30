class ApiException(Exception):
    pass

class DBConnectionIssue(ApiException):
    def __str__(self):
        return "Database Error: %s" % self.message

class DBError(ApiException):
    def __init__(self, *args):
        # args can have 1 or 2 parameters
        try:
            self.errno = args[0]
            self.message = args[1]
        except IndexError:
            self.message = args[0]

    def __str__(self):
        return self.message
