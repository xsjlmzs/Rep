FROM ubuntu:20.04
LABEL author="hsj"
#安装依赖包
RUN apt update -y &&\
    yum install -y git cmake pkg-config
    
    
    
    
    
    
    
    
    
    
#创建用户，openGauss基于postgresql，和postresql一样，不能在root用户下启动
RUN echo '12345678' | passwd --stdin root &&\
    groupadd dbgroup &&\
    useradd -g dbgroup omm
#切换工作目录，下面几条命令都在此目录下执行
USER omm
WORKDIR /home/omm
#下载openGauss-third_party_binarylibs解压
RUN wget https://opengauss.obs.cn-south-1.myhuaweicloud.com/2.0.0/openGauss-third_party_binarylibs.tar.gz &&\
    tar -zxf openGauss-third_party_binarylibs.tar.gz &&\
    mv openGauss-third_party_binarylibs binarylibs
#clone源码，编译，编译后安装在/home/omm/openGauss-server/mppdb_temp_install下
RUN git clone -b 2.0.0 https://gitee.com/opengauss/openGauss-server.git &&\
    cd openGauss-server &&\
    sh build.sh -m debug -3rd /home/omm/binarylibs &&\
    cp -r simpleInstall/ mppdb_temp_install/
#清理垃圾文件
RUN rm openGauss-third_party_binarylibs.tar.gz
