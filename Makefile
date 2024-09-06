.PHONY: regen-api

regen-api:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/replication.proto proto/partitioning.proto proto/api.proto proto/basic_leaderless.proto proto/routing.proto proto/tapestry.proto proto/raft.proto
