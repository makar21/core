# Local test

```shell
bin/download-cifar10
# Test train local
python manage-tasks.py -c add \
    --local=1 \
    --workers=1 \
    --epochs=1 \
    --batch=32 \
    --dataset=examples/keras/cifar10 \
    --path=examples/keras/cifar10/resnet.py
```