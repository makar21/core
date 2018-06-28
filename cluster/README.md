# Local cluster setup

Initialize 4 Tendermint nodes.

    sudo python3 init_tm.py

This creates tmdata dir with subdirectories for each node.

Generate docker-compose file:

    python3 generate_docker_compose_file.py

Remove the existing tmdata dir in the bigchaindb dir:

    rm -r ../../bigchaindb/tmdata

Move tmdata and docker-compose.yml to the bigchaindb dir:

    sudo mv tmdata ../../bigchaindb
    mv docker-compose.yml ../../bigchaindb

Now you can run `docker-compose up` in the bigchaindb dir to start the cluster.
