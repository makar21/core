#!/bin/bash
docker run -it --rm -v $PWD/testnet:/tendermint tendermint/tendermint:0.22.4 testnet --hostname-prefix tatau --o tatau --v 4 --n 1