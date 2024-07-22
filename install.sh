apt-get update && apt-get upgrade -y

# Install Rust
curl https://sh.rustup.rs -sSf | sh
. "$HOME/.cargo/env"            # For sh/bash/zsh/ash/dash/pdksh

# Install clang
apt-get install clang -y

# Install lld
apt-get install lld -y

# Install perf
apt-get install linux-tools-common linux-tools-generic linux-tools-`uname -r` -y

# Install hotspot
apt-get install hotspot -y

# Install heaptrack
apt-get install heaptrack heaptrack-gui -y

# Update python and pip
apt-get install python3 python3-pip -y