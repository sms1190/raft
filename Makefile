proto-gen:
	protoc --go_out=./pb --go_opt=paths=source_relative --go-grpc_out=./pb --go-grpc_opt=paths=source_relative ./proto/*.proto
start-server:
	go run .\main.go -port 8080 -id node1 -peers 0.0.0.0:3000 -peers 0.0.0.0:5000
start-server2:
	go run .\main.go -port 3000 -id node2 -peers 0.0.0.0:8080 -peers 0.0.0.0:5000
start-server3:
	go run .\main.go -port 5000 -id node3 -peers 0.0.0.0:8080 -peers 0.0.0.0:3000