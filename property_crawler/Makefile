jupyter:
	docker run --rm -it -p 8888:8888 -v $(shell pwd):/usr/src/app property_finder_image_docker 

bash:
	docker run --rm -it -v $(shell pwd):/usr/src/app --env-file ./.env property_finder_image_docker /bin/bash

build:
	docker build -t property_finder_image_docker .