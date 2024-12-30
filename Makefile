run-zookeeper:
	docker run -d -p 2181:2181 --name zookeeper zookeeper

build:
	docker compose run --build
