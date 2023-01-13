FROM centos:7

ARG PYTHON_VER=3.9.16
RUN yum -y install openssl-devel bzip2-devel libffi-devel xz-devel wget gcc make
WORKDIR /opt
RUN wget https://www.python.org/ftp/python/$PYTHON_VER/Python-$PYTHON_VER.tgz
RUN tar -xzf Python-$PYTHON_VER.tgz
WORKDIR /opt/Python-$PYTHON_VER
RUN ./configure LDFLAGS="-Wl,--rpath=/usr/local/lib"
RUN make -j4 altinstall
# RUN pip3.9 install virtualenv
COPY ./requirements*.txt ./
RUN pip3.9 install -r requirements_dev.txt

FROM centos:7
RUN yum -y install epel-release && yum clean all
RUN yum install -y chromedriver chromium && yum clean all
COPY --from=0 /usr/local /usr/local
