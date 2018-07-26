SHELL=/bin/bash
NAME=tataucloud/$(APP)
TAG=master
ALIAS=latest


.PHONY: pre-build docker-build post-build clean build tag release push do-push post-push

build: pre-build docker-build post-build

pre-build:

post-build: clean

docker-build:
	# echo "branch $(BRANCH)"
	docker build . -t $(NAME):$(TAG) -f docker/images/$(APP)/Dockerfile --force-rm --pull

release: build tag push

push: do-push post-push

tag:
	docker tag $(NAME):$(TAG) $(NAME):$(ALIAS)


do-push:
	docker push $(NAME):$(TAG)
	docker push $(NAME):$(ALIAS)

post-push:

clean: