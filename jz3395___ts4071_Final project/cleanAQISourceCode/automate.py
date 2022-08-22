import os
#I've already mkdir lab3, and put the data.txt there.
os.system("hadoop fs -rm -r finalProject/output")
os.system("javac -classpath `hadoop classpath` AQIMapper.java")
os.system("javac -classpath `hadoop classpath` AQIReducer.java")
os.system("javac -classpath `hadoop classpath`:. AQI.java")
os.system("jar cvf AQIjar.jar *.class")
os.system("hadoop jar AQIjar.jar AQI /user/jz3395/finalProject/combinedAQI.txt /user/jz3395/finalProject/output")