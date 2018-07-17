wget http://it.apache.contactlab.it/maven/maven-3/3.5.0/binaries/apache-maven-3.5.0-bin.tar.gz
tar -xvf apache-maven-3.5.0-bin.tar.gz
rm -iv apache-maven-3.5.0-bin.tar.gz

cd apache-maven-3.5.0/bin/

export M2_HOME=`pwd`
export PATH=$PATH:$M2_HOME
echo $M2_HOME
which mvn
