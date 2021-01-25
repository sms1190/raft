proto-gen:
	protoc --go_out=./pb --go_opt=paths=source_relative --go-grpc_out=./pb --go-grpc_opt=paths=source_relative ./proto/*.proto
start-server:
	go run .\main.go -port 8080 -restport 80 -id node1 -peers 0.0.0.0:8081 -peers 0.0.0.0:8082
start-server2:
	go run .\main.go -port 8081 -restport 81 -id node2 -peers 0.0.0.0:8080 -peers 0.0.0.0:8082
start-server3:
	go run .\main.go -port 8082 -restport 82 -id node3 -peers 0.0.0.0:8080 -peers 0.0.0.0:8081