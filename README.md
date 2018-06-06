# Local setup

For setting up the project, run the following commands.

    virtualenv .venv -p python3
    source .venv/bin/activate
    pip install -r requirements.txt

Read [here](https://docs.bigchaindb.com/projects/py-driver/en/latest/quickstart.html) about BigchainDB driver dependencies.

You will also need a BigchainDB node.

After you finish the setup, you can use the db module to check everything.

# Get BigchainDB Server

To get BigchainDB Server, do the following:

    git clone https://github.com/bigchaindb/bigchaindb.git
    cd bigchaindb
    git checkout v1.3.0

Now open docker-compose.yml and change `ports` to expose 9984 and 9985:
    ports:
      - "9984:9984"
      - "9985:9985"

To run BigchainDB Server:

    docker-compose up
