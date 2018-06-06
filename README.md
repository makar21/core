# Local setup

For setting up the project, run the following commands.

```shell
virtualenv .venv -p python3
source .venv/bin/activate
pip install -r requirements.txt
```

Read [here](https://docs.bigchaindb.com/projects/py-driver/en/latest/quickstart.html) about BigchainDB driver dependencies.

You will also need a BigchainDB node.

After you finish the setup, you can use the db module to check everything.

# Get BigchainDB Server

To get BigchainDB Server, do the following:

```shell
git clone https://github.com/bigchaindb/bigchaindb.git
cd bigchaindb
git checkout v1.3.0
```

Now open docker-compose.yml and change `ports` to expose 9984 and 9985:
```
ports:
  - "9984:9984"
  - "9985:9985"
```

To run BigchainDB Server:

    docker-compose up

# Generating keys

Encryption is used for some of the data. You will need to generate keys for producer and worker.

Create `keys` folder:

```shell
mkdir keys
```

Generate producer key:

```shell
python encryption.py --generate-key keys/producer.pem
```

Generate worker key:

```shell
python encryption.py --generate-key keys/worker.pem
```

# Running worker

To run worker:

```shell
python worker.py
```

**How it works**

The worker connects to WebSocket Event Stream API and listens to new transactions. Once a transaction occurs, it retrieves the transaction from BigchainDB. Then, depending on what the transaction is, the worker can continue to additional action:

* If it’s a **Task declaration**, the worker sends a request to the producer’s API to let the producer know that it is ready for doing the task.

* If it’s a **Task assignment**, the worker works on the task, and then creates a **Task processing** asset in BigchainDB.


# Running producer

To run producer:

```shell
python producer.py
```

**How it works**

The producer creates a **Task declaration** asset in BigchainDB containing its API URL, waits for a worker to make a call to its API, and then creates a **Task assignment** asset in BigchainDB.
