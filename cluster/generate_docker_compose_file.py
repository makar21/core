start = """version: '3'

services:
"""

node_services = """  mongodb-n{node_num}:
    image: mongo:3.6
    expose:
      - "27017"
    command: mongod
  bigchaindb-n{node_num}:
    depends_on:
      - mongodb-n{node_num}
      - tendermint-n{node_num}
    build:
      context: .
      dockerfile: Dockerfile-dev
      args:
        backend: localmongodb
    volumes:
      - ./bigchaindb:/usr/src/app/bigchaindb
      - ./tests:/usr/src/app/tests
      - ./docs:/usr/src/app/docs
      - ./htmlcov:/usr/src/app/htmlcov
      - ./setup.py:/usr/src/app/setup.py
      - ./setup.cfg:/usr/src/app/setup.cfg
      - ./pytest.ini:/usr/src/app/pytest.ini
      - ./tox.ini:/usr/src/app/tox.ini
    environment:
      BIGCHAINDB_DATABASE_BACKEND: localmongodb
      BIGCHAINDB_DATABASE_HOST: mongodb-n{node_num}
      BIGCHAINDB_DATABASE_PORT: 27017
      BIGCHAINDB_SERVER_BIND: 0.0.0.0:9984
      BIGCHAINDB_WSSERVER_HOST: 0.0.0.0
      BIGCHAINDB_WSSERVER_ADVERTISED_HOST: bigchaindb-n{node_num}
      BIGCHAINDB_TENDERMINT_HOST: tendermint-n{node_num}
      BIGCHAINDB_TENDERMINT_PORT: 46657
    expose:
      - "46658"
    ports:
      - "{bigchaindb_port}:9984"
      - "{bigchaindb_ws_port}:9985"
    command: '.ci/entrypoint.sh'
  tendermint-n{node_num}:
    image: tendermint/tendermint:0.19.9
    volumes:
      - ./tmdata/n{node_num}:/tendermint
    expose:
      - "46656"
      - "46657"
    command: node
"""

def generate_docker_compose_file():
    nodes_num = 4

    with open('docker-compose.yml', 'w') as f:
        f.write(start)
        for node_num in range(nodes_num):
            f.write(node_services.format(
                node_num=node_num,
                bigchaindb_port=9200+node_num,
                bigchaindb_ws_port=9300+node_num
            ))

if __name__ == '__main__':
    generate_docker_compose_file()
