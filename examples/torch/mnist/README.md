# Torch MNIST Example

## Download Dataset

```shell
cd examples/torch/mnist
curl https://s3.amazonaws.com/tatau-public/datasets/torch/mnist/mnist.zip -O
unzip mnist.zip
cd ../../../
```

## Run Train Local

```shell
python manage-tasks.py -c add \
    --local=1 \
    --workers=1 \
    --epochs=10 \
    --batch=32 \
    --dataset=examples/torch/mnist \
    --path=examples/torch/mnist/cnn.py
```

Output

```
Epoch #1: Loss: 0.6919 Acc: 77.53
Epoch #2: Loss: 0.3032 Acc: 91.00
Epoch #3: Loss: 0.2477 Acc: 92.75
Epoch #4: Loss: 0.2150 Acc: 93.68
Epoch #5: Loss: 0.1918 Acc: 94.44
Epoch #6: Loss: 0.1818 Acc: 94.72
Epoch #7: Loss: 0.1735 Acc: 94.97
Epoch #8: Loss: 0.1597 Acc: 95.40
Epoch #9: Loss: 0.1552 Acc: 95.41
Epoch #10: Loss: 0.1511 Acc: 95.60

Test set: Average loss: 0.03908432, Accuracy: 9869/10000 (0.9869%)

```
## Run Train Remote

```shell
python manage-tasks.py -c add \
    --local=0 \
    --workers=1 \
    --epochs=10 \
    --batch=32 \
    --dataset=examples/torch/mnist \
    --path=examples/torch/mnist/cnn.py
```