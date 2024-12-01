#!/bin/bash

# Find the Java installation path
JAVA_PATH=$(update-alternatives --config java | grep -oP '(?<=/usr/lib/jvm/)[^/]+(?=/bin/java)' | head -n 1)

# Set JAVA_HOME environment variable
echo "export JAVA_HOME=/usr/lib/jvm/$JAVA_PATH" >> ~/.bashrc
echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> ~/.bashrc

####### change your env activate file path here #######
# set this to your env activate path
echo "export JAVA_HOME=/usr/lib/jvm/$JAVA_PATH" >> /home/akshat7/CS511_project/.project_env/bin/activate
echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> /home/akshat7/CS511_project/.project_env/bin/activate

# Source the .bashrc file to apply changes
source ~/.bashrc

# Verify JAVA_HOME
echo $JAVA_HOME
