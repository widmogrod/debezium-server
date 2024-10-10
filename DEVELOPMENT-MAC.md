# Development
Few tips and tricks how to get started, with zero java know how on Apple M-series computer.

```
git checkout v2.7.1.Final

brew install openjdk 
sudo ln -sfn $HOMEBREW_PREFIX/opt/openjdk/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk.jdk

brew install maven
 
# https://podman.io/docs/installation
brew install podman
podman machine init --cpus 6 -m 10 --rootful --user-mode-networking
podman machine start

## for colima
colima start --network-address

## setup env variables
cp .envrc.colima .envrc
source .envrc

### colima fixes and issues
issue: https://github.com/abiosoft/colima/issues/449
fix:
```
colima ssh << EOF
# Switch to superuser
sudo su -
sudo ip addr add 192.168.106.2/24 dev col0
sudo ip link set col0 up
sudo ip route add default via 192.168.106.1
sudo ip link set col0 down
sudo ip link set col0 up
ip addr show col0
ip route show
EOF
```


# follow readme.md installation instructions

# you may need to run this command few times to make tests pass

./mvnw clean verify -DskipITs=false -am -pl debezium-server-cratedb 
```