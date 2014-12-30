import os, sys
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
sys.path.append(ROOT_PATH)

from memsql_loader.api.caller import ApiCaller
api = ApiCaller()
