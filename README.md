# Local setup

For setting up the project, run the following commands.

```shell
virtualenv .venv -p python3
source .venv/bin/activate
pip install -r requirements.txt
```

Read [here](https://docs.bigchaindb.com/projects/py-driver/en/latest/quickstart.html) about BigchainDB driver dependencies.

You will also need a BigchainDB node.

# Get BigchainDB Server

To get BigchainDB Server, do the following:

```shell
git clone https://github.com/bigchaindb/bigchaindb.git
cd bigchaindb
git checkout v1.3.0
```

Now open `docker-compose.yml` and change `ports` to expose 9984 and 9985:
```
ports:
  - "9984:9984"
  - "9985:9985"
```

To run BigchainDB Server:

    docker-compose up

# DB

BigchainDB assets can be created, updated and retrieved with the `db` module.

A key pair is generated automatically when the `DB` class is initialized.

# Encryption

Tasks and results are encrypted with the `encryption` module. The encryption keys are automatically generated when the `Encryption` class is initialized.

The data is encrypted using a hybrid encryption scheme. We use RSA with PKCS#1 [OAEP](https://en.wikipedia.org/wiki/Optimal_asymmetric_encryption_padding) for asymmetric encryption of an AES session key. After it, the session key is used to encrypt the actual data.

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


# Install IPFS

[Install instructions](https://ipfs.io/docs/install/)

after install run
```shell
ipfs daemon --init
```

[Doker instructions](https://hub.docker.com/r/jbenet/go-ipfs/), read Docker Usage

