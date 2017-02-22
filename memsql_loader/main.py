import os

try:
    import apsw         # noqa
except ImportError:
    import textwrap
    print(textwrap.dedent("""\

        Almost there!

        MemSQL Loader requires the APSW library to be installed.  It is not
        available on PyPi so you must install it manually.  Here is a one-liner
        for the pip command:

            pip install \\
                -e "git+git://github.com/rogerbinns/apsw.git@26b61c39a98db3ccad7f852adc944b0b8e94c242#egg=apsw" \\
                --global-option="fetch" --global-option="--sqlite" --global-option="--missing-checksum-ok" \\
                --global-option="build" --global-option="--enable-all-extensions"
        """))
    os._exit(1)

from memsql_loader.util import config
from memsql_loader.db import pool

def main():
    options = config.load_options()

    try:
        options.command(options)
    finally:
        pool.close_connections()
