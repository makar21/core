# Tatau Core Stack
Core stack for mining tokens and producing training jobs


## Preparing Environment

```shell
cp .env.example .env

RING="your ring key" && echo -e "\nRING=$RING" >> .env

# TODO: Describe CONTRACT_ADDESS & ACCOUNT_ADDRESS

bin/start bigchaindb_this
```


Wait for Blockchain Synchronization

```shell
bin/logs tendermint_this
```

Check that block has a latest height

`TODO:` describe how to use tendermint api to check sync state


## Start Mining

```shell
bin/start <worker_cpu | worker_gpu>

```

## Train Model

```shell
bin/start producer
```


#### Deploy Train Job via Local Producer

```shell
# attach to producer tty
bin/compose exec producer bash

cd examples/keras/cifar10/

wget https://s3.amazonaws.com/tatau-public/datasets/cifar10/x_test.npy
wget https://s3.amazonaws.com/tatau-public/datasets/cifar10/y_test.npy
wget https://s3.amazonaws.com/tatau-public/datasets/cifar10/x_train.npy
wget https://s3.amazonaws.com/tatau-public/datasets/cifar10/y_train.npy

cd /app

python add-task.py \
    --local=0 \
    --workers=1 \
    --epochs=1 \
    --batch=32 \
    --dataset=examples/keras/cifar10 \
    --path=examples/keras/cifar10/resnet.py
```

In output you will see **Train job created**
```
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

#### Start Training Monitor
```shell
python train-monitor.py -t d3a2b3e05bbf581078cb16bfd460b15479e11d1ca5203c31105cba11ef3c01d6
```




# Examples

[Keras Cifar10 ResNet V2](examples/keras/cifar10/README.md)

[Keras MNIST](examples/keras/mnist/README.md)

[PyTorch MNIST](examples/torch/mnist/README.md)

[PyTorch CIFAR10](examples/torch/cifar10/README.md)

# Local setup

For setting up the project, run the following commands.

```shell
virtualenv .venv -p python3
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
docker-compose up

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

# Escrow Usage Example

Set actual CONTRACT_ADDRESS in .env file

```python
from tatau_core.contract import Contract
from tatau_core import web3
from web3.personal import Personal


with open("parity/sandbox/keys/account.json") as keyfile:
    encrypted_key = keyfile.read()

with open("parity/wallet/account.pass") as passfile:
    keyfile_pass = passfile.read()

private_key = web3.eth.account.decrypt(encrypted_key, keyfile_pass)

account = web3.eth.account.privateKeyToAccount(private_key)

personal = Personal(web3=web3)

personal.unlockAccount(account.address, keyfile_pass)


web3.eth.defaultAccount = account.address

contract = Contract()
job_budget = web3.toWei("1", "ether")

task_declaration_id = "your job key"

# Issue Job
personal.unlockAccount(account.address, keyfile_pass)
job_id = contract.issue_job(task_declaration_id=task_declaration_id, value=job_budget)

# Deposit Job
personal.unlockAccount(account.address, keyfile_pass)
contract.deposit(task_declaration_id, job_budget)

# Check Job Balance
balance = contract.get_job_balance(task_declaration_id)

balance_eth = web3.fromWei(balance, 'ether')
print("Job Balance: {} ETH".format(balance_eth))

worker_reward = web3.toWei("0.5", "ether")

# Distribute
personal.unlockAccount(account.address, keyfile_pass)
contract.distribute(task_declaration_id, workers=[account.address], amounts=[worker_reward])

# Finish Job
personal.unlockAccount(account.address, keyfile_pass)
contract.finish_job(task_declaration_id)

print("Job Finished")

# Check that Issuer refunded
balance = contract.get_job_balance(task_declaration_id)
balance_eth = web3.fromWei(balance, 'ether')
print("Job Balance After Finish: {} ETH".format(balance_eth))
```