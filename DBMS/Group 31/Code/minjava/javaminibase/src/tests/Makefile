JDKPATH = /usr
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# test buffer manager


JoinsDriver: JoinsDriver.java
	$(JAVAC) TestDriver.java IEJoinDriver_2c.java IEJoinDriver_2d.java IESelfJoin.java IEJoinTask1.java JoinsDriver.java
jttest:JoinsDriver
	$(JAVA) tests.JoinsDriver 
