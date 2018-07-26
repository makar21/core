# Setup worker

```shell
cp .env.example .env

cp docker/docker.env.example docker/docker.env

RING="your ring key" && echo -e "\nRING=$RING" >> .env

bin/core-up <cpu|gpu> #
```

# Deploy train job
```shell

cp .env.example .env

cp docker/docker.env.example docker/docker.env

RING="your ring key" && echo -e "\nRING=$RING" >> .env

docker-compose -f common.yml -f producer.yml up -d

# Fix tendermint volume permissions
sudo chmod -R 755 docker/volumes

export WORKERS=10
docker exec -it tatau_producer_1 sh -c "\
cd examples/keras/cifar10/ && \
wget https://s3.amazonaws.com/tatau-public/datasets/cifar10/x_test.npy && \
wget https://s3.amazonaws.com/tatau-public/datasets/cifar10/y_test.npy && \
wget https://s3.amazonaws.com/tatau-public/datasets/cifar10/x_train.npy && \
wget https://s3.amazonaws.com/tatau-public/datasets/cifar10/y_train.npy && \
cd /app && \
python add-task.py \
    --local=0 \
    --workers=$WORKERS \
    --epochs=1 \
    --batch=32 \
    --dataset=examples/keras/cifar10 \
    --path=examples/keras/cifar10/resnet.py"
```

# Examples

[Keras Cifar10 ResNet V2](examples/keras/cifar10/README.md)

[Keras MNIST](examples/keras/mnist/README.md)

# Local setup

For setting up the project, run the following commands.

```shell
virtualenv .venv -p python3
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
docker-compose up

```

# Start train and monitor

Start docker containers

```shell
bin/core-up cpu

```

Add task

```shell
docker exec -it tatau_core_producer_1 python  add-task.py -l 0
```

In output you will see:
```shell
2018-07-26 14:28:21,701 P28 INFO |__main__| Upload weights to IPFS
2018-07-26 14:28:21,912 P28 INFO |root| Creating dataset
2018-07-26 14:28:22,047 P28 INFO |root| Split dataset to 60 batches
2018-07-26 14:28:22,105 P28 INFO |root| Upload dataset to IPFS
2018-07-26 14:28:22,508 P28 INFO |root| Dataset was uploaded
2018-07-26 14:28:23,548 P28 INFO |__main__| Dataset created: <Dataset: 69d878d5bef2248255c87b805e4dea605e12368843a2fb89dbbeeafad77f28b2>
2018-07-26 14:28:23,549 P28 INFO |__main__| Create model
2018-07-26 14:28:24,347 P28 DEBUG |__main__| Model created: <TrainModel: b158c1ef62dc280c79ea43bd27862d64744fa26737781d582afa02887731d0da>
2018-07-26 14:28:24,347 P28 INFO |__main__| Create train job
2018-07-26 14:28:25,379 P28 DEBUG |__main__| Train job created: <TaskDeclaration: d3a2b3e05bbf581078cb16bfd460b15479e11d1ca5203c31105cba11ef3c01d6>
```

Copy Task Declaration ID, in current case it: `d3a2b3e05bbf581078cb16bfd460b15479e11d1ca5203c31105cba11ef3c01d6`

And run:
```shell
docker exec -it tatau_core_producer_1 python  train-monitor.py -t d3a2b3e05bbf581078cb16bfd460b15479e11d1ca5203c31105cba11ef3c01d6
```


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

* If it’s a **Task assignment**, the worker works on the task, and then updates the asset in BigchainDB.

# Running verifier

To run verifier:

```shell
python verifier.py
```

**How it works**

The verifier connects to WebSocket Event Stream API and listens to new transactions. Once a transaction occurs, it retrieves the transaction from BigchainDB. Then, depending on what the transaction is, the verifier can continue to additional action:

* If it’s a **Verification declaration**, the verifier sends a request to the producer’s API to let the producer know that it is ready for verifying the task.

* If it’s a **Verification assignment**, the verifier verifies the task, and then updates the asset in BigchainDB.

# Running producer

To run producer:

```shell
python producer.py
```

**How it works**

The producer creates a **Task declaration** asset in BigchainDB containing its API URL, waits for a worker to make a call to its API, and then creates a **Task assignment** asset in BigchainDB. The asset’s recipient is the worker.

After a worker adds a result, the producer creates a **Verification declaration** asset in BigchainDB containing its API URL, waits for a verifier to make a call to its API, and then creates a **Verification assignment** asset in BigchainDB. The asset’s recipient is the verifier.
