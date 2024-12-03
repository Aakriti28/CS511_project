#!/bin/bash

# Find the Java installation path
JAVA_PATH=$(update-alternatives --config java | grep -oP '(?<=/usr/lib/jvm/)[^/]+(?=/bin/java)' | head -n 1)
echo $CONDA_PREFIX

# Create conda environment activation directory if it doesn't exist
mkdir -p $CONDA_PREFIX/etc/conda/activate.d
mkdir -p $CONDA_PREFIX/etc/conda/deactivate.d

# Set JAVA_HOME environment variable
echo "export JAVA_HOME=/usr/lib/jvm/$JAVA_PATH" >> ~/.bashrc
echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> ~/.bashrc

# ####### change your env activate file path here #######
# # set this to your env activate path
# echo "export JAVA_HOME=/usr/lib/jvm/$JAVA_PATH" >> /home/akshat7/CS511_project/.project_env/bin/activate
# echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> /home/akshat7/CS511_project/.project_env/bin/activate

# # Source the .bashrc file to apply changes
# source ~/.bashrc

# # Verify JAVA_HOME
# echo $JAVA_HOME

# Create activation script
cat << EOF > $CONDA_PREFIX/etc/conda/activate.d/java_env.sh
#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/$JAVA_PATH
export PATH=\$PATH:\$JAVA_HOME/bin
EOF

# Create deactivation script
cat << EOF > $CONDA_PREFIX/etc/conda/deactivate.d/java_env.sh
#!/bin/bash
unset JAVA_HOME
EOF

# Make scripts executable
chmod +x $CONDA_PREFIX/etc/conda/activate.d/java_env.sh
chmod +x $CONDA_PREFIX/etc/conda/deactivate.d/java_env.sh

# Verify JAVA_HOME (requires reactivating environment)
echo "Please reactivate your conda environment with:"
echo "conda deactivate && conda activate <your_env_name>"
echo "Then verify JAVA_HOME with: echo \$JAVA_HOME"

# Create conda environment activation/deactivation directories
mkdir -p $CONDA_PREFIX/etc/conda/activate.d
mkdir -p $CONDA_PREFIX/etc/conda/deactivate.d

# Create activation script
cat << 'EOF' > $CONDA_PREFIX/etc/conda/activate.d/hadoop_env.sh

export HADOOP_HOME=/shared/data/aa117/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH
EOF

# Create deactivation script
cat << 'EOF' > $CONDA_PREFIX/etc/conda/deactivate.d/hadoop_env.sh
#!/bin/bash
unset HADOOP_HOME
unset HADOOP_INSTALL
unset HADOOP_MAPRED_HOME
unset HADOOP_COMMON_HOME
unset HADOOP_HDFS_HOME
unset YARN_HOME
unset HADOOP_COMMON_LIB_NATIVE_DIR
EOF

# Make scripts executable
chmod +x $CONDA_PREFIX/etc/conda/activate.d/hadoop_env.sh
chmod +x $CONDA_PREFIX/etc/conda/deactivate.d/hadoop_env.sh

# Print verification instructions
echo "Environment variables have been set up."
echo "To verify, please run:"
echo "conda deactivate && conda activate <your_env_name>"
echo "Then verify with:"
echo "echo \$HADOOP_HOME"
echo "echo \$PATH"
echo "echo \$LD_LIBRARY_PATH"