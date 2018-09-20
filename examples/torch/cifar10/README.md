# Torch CIFAR10 Example

## Download Dataset

```shell
cd examples/torch/cifar10
wget https://s3.amazonaws.com/tatau-public/datasets/cifar10_unnormalized/x_test.npy
wget https://s3.amazonaws.com/tatau-public/datasets/cifar10_unnormalized/y_test.npy
wget https://s3.amazonaws.com/tatau-public/datasets/cifar10_unnormalized/x_train.npy
wget https://s3.amazonaws.com/tatau-public/datasets/cifar10_unnormalized/y_train.npy
cd ../../../
```

## Run Train Local

### CNN

```shell
python manage-tasks.py -c add \
    --local=1 \
    --workers=1 \
    --epochs=10 \
    --batch=32 \
    --dataset=examples/torch/cifar10 \
    --path=examples/torch/cifar10/cnn.py \
    --name=cifar10_cnn
```

### ResNet

```shell
python manage-tasks.py -c add \
    --local=1 \
    --workers=3 \
    --epochs=2 \
    --batch=32 \
    --dataset=examples/torch/cifar10 \
    --path=examples/torch/cifar10/resnet.py \
    --name=cifar10_resnet18
```

## Run Train Remote

```shell
python manage-tasks.py -c add \
    --local=0 \
    --workers=1 \
    --epochs=10 \
    --batch=32 \
    --dataset=examples/torch/cifar10 \
    --path=examples/torch/cifar10/cnn.py \
    --name=cifar10_cnn
```