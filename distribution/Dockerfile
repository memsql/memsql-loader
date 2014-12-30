FROM tianon/centos:5.8
MAINTAINER carl@memsql.com

# dependencies
RUN yum install -y make wget gcc gcc-c++ bzip2 bzip2-devel zlib-devel openssl-devel mysql-devel which patch autoconf curl-devel expat-devel gettext-devel sqlite3-devel httpd-devel which libffi rpm-build

# install a newer version of ncurses
RUN wget -q --no-check-certificate -O /tmp/ncurses.tar.gz http://ftp.gnu.org/pub/gnu/ncurses/ncurses-5.9.tar.gz && \
    cd /tmp && tar -xzf /tmp/ncurses.tar.gz && \
    cd /tmp/ncurses-5.9/ && \
    ./configure --prefix=/usr/local --with-shared --without-debug --enable-pc-files --with-terminfo-dirs="/etc/terminfo:/lib/terminfo:/usr/share/terminfo" --with-default-terminfo-dir=/usr/share/terminfo && \
    make -j 12 && make install

# install libyaml
WORKDIR /usr/src
RUN wget http://pyyaml.org/download/libyaml/yaml-0.1.4.tar.gz
RUN tar zxf yaml-0.1.4.tar.gz
WORKDIR yaml-0.1.4
RUN ./configure --prefix=/usr/local
RUN make -j 12
RUN make install

# install ruby 1.9.3
WORKDIR /usr/src
RUN wget -q --no-check-certificate http://ftp.ruby-lang.org/pub/ruby/1.9/ruby-1.9.3-p0.tar.gz
RUN tar zxf ruby-1.9.3-p0.tar.gz
WORKDIR /usr/src/ruby-1.9.3-p0
RUN ./configure --prefix=/usr/local --disable-install-doc
RUN make -j 12
RUN make install
WORKDIR /usr/src

# install fpm
RUN gem install fpm

# install python2.7.8
RUN wget -q --no-check-certificate -O /tmp/python-2.7.8.tar.gz http://www.python.org/ftp/python/2.7.8/Python-2.7.8.tgz && \
    cd /tmp && tar -xzf /tmp/python-2.7.8.tar.gz && \
    cd /tmp/Python-2.7.8/ && \
    ./configure --enable-shared --prefix=/usr/local CPPFLAGS="-I/usr/local/include -I/usr/local/include/ncurses" LDFLAGS=-Wl,-rpath=/usr/local/lib && \
    make -j 12 && make altinstall

# install latest setuptools
RUN wget -q --no-check-certificate -O /tmp/ez_setup.py https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py && \
    cd /tmp && /usr/local/bin/python2.7 /tmp/ez_setup.py --insecure

# install latest pip
RUN wget -q --no-check-certificate -O /tmp/get-pip.py https://raw.github.com/pypa/pip/master/contrib/get-pip.py && \
    /usr/local/bin/python2.7 /tmp/get-pip.py

# install pip 1.5.6
RUN pip install pip==1.5.6

# install pyinstaller
RUN /usr/local/bin/pip2.7 install pyinstaller==2.1

# install latest libcurl
RUN cd /tmp && wget -q --no-check-certificate -O /tmp/curl.tar.gz http://curl.haxx.se/download/curl-7.37.0.tar.gz && \
    tar -xzf curl.tar.gz && \
    cd curl* && \
    ./configure && make -j 12 && make install

# install readline
RUN CPPFLAGS="-I/usr/local/include -I/usr/local/include/ncurses" LDFLAGS="-Wl,-rpath=/usr/local/lib,-rpath=/usr/local/include/ncurses" /usr/local/bin/easy_install-2.7 readline

# install git from source
RUN wget -q --no-check-certificate -O /tmp/git-2.2.0.tar.gz https://www.kernel.org/pub/software/scm/git/git-2.2.0.tar.gz && \
    ls /tmp && cd /tmp && tar xzvf /tmp/git-2.2.0.tar.gz && \
    cd /tmp/git-2.2.0 && \
    ./configure && \
    make && \
    make install
