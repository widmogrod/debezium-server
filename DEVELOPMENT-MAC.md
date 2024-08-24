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

# https://java.testcontainers.org/supported_docker_environment/#colima
export DOCKER_HOST=unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
export TESTCONTAINERS_RYUK_DISABLED=true

## for colima
colima start --network-address
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
export TESTCONTAINERS_HOST_OVERRIDE=$(colima ls -j | jq -r 'select(.name == "default") | .address')
export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"
export TESTCONTAINERS_RYUK_DISABLED=true

# follow readme.md installation instructions

# you may need to run this command few times to make tests pass

./mvnw clean verify -DskipITs=false -am -pl debezium-server-cratedb 
```