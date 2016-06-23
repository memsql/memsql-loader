# vim: set ft=python:

projpath = os.path.dirname(os.path.abspath(SPEC))

a = Analysis(['../bin/memsql-loader'], pathex=['../'])

a.binaries += [('libcurl.so.4.3.0', '/usr/local/lib/libcurl.so.4.3.0', 'BINARY')]
a.binaries += [('libcurl.so', '/usr/local/lib/libcurl.so', 'BINARY')]
a.binaries += [('libcurl.so.4', '/usr/local/lib/libcurl.so.4', 'BINARY')]
a.binaries += [('libldap-2.3.so.0', '/usr/lib64/libldap-2.3.so.0', 'BINARY')]
a.binaries += [('libldap_r-2.3.so.0', '/usr/lib64/libldap_r-2.3.so.0', 'BINARY')]
a.binaries += [('liblber-2.3.so.0', '/usr/lib64/liblber-2.3.so.0', 'BINARY')]
a.binaries += [('libidn.so.11', '/usr/lib64/libidn.so.11', 'BINARY')]
a.binaries += [('libidn.so', '/usr/lib64/libidn.so', 'BINARY')]
a.binaries += [('libsasl2.so.2', '/usr/lib64/libsasl2.so.2', 'BINARY')]
a.binaries += [('libsasl2.so', '/usr/lib64/libsasl2.so', 'BINARY')]
a.binaries += [('mmap.so', '/usr/lib/python2.7/lib-dynload/mmap.x86_64-linux-gnu.so', 'BINARY')]

pyz = PYZ(a.pure)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          debug=False,
          strip=None,
          upx=False,
          name="memsql-loader",
          console=True )
