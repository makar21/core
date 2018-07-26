# Local test

```shell
# Test train local
docker exec tatau_core_worker_1 \
    python add-task.py \
    --local=1 --workers=1 --epochs=1 --batch=128 \
    --dataset=examples/keras/mnist \
    --path=examples/keras/mnist/mlp.py
```