from memsql_loader.util import bootstrap

class Command(object):
    def __init__(self, options):
        self.options = options
        self.ensure_bootstrapped()
        self.run()

    @staticmethod
    def configure(parser, subparsers):
        raise NotImplemented('Every command needs a static configure(...) method')

    def ensure_bootstrapped(self):
        if not bootstrap.check_bootstrapped():
            bootstrap.bootstrap()

    def run():
        raise NotImplemented('Every command needs a run method, otherwise not much is going to happen')
