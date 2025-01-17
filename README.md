# Modist
[![](https://skillicons.dev/icons?i=go,docker)](https://skillicons.dev)

Store your data on Modist, a document-based Daas(Database as a service) that support docker deployment.

For more detail, please read this [developer blog](https://xjhao.studio/02/25/2023/modist/).

Docker swarm supporting on the way...

Created by Yutong Li(yli195) and Xingjian Hao(xhao9). Backup and updated from private reporsitory.

## What is is
Modist, or Modular Distributed Storage, is a distributed databse that supports multiple synchoronization model setups for each functionality domain. Common issues for distributed systems are addressed with proven solutions from academia and industry. 

Modist is built with BoltDB, gRPC, and Protobuf. The system overall applies a raft schema, and each nodes meantained local replication and partitioning for data safety.

![Raft](src/raft.png)

![Node](src/system.png)

## Known Issues
Please check Issue tab for more information.

## Usage

Modist docker image is available at [here](https://hub.docker.com/layers/ptjohn0122/modist/latest/images/sha256:c5a2d62ba6490b777db17e1abfd1da8f5ed11beedb0bfa84eb3d3d82b9efe828?uuid=12790BD7-25E7-45A5-A81B-26B2C708E1A3).

Usage of Modist Docker Image:

```shell
docker pull PTJohn0122/modist:latest
```

While docker is provided as general solution, modist also provide local package for development and integration. To use,

```shell
git clone https://github.com/PTJohn0122/Modular_Distributed_Storage.git
go build
```