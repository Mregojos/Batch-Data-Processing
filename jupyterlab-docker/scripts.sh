# To build and run jupyterlab container
docker build -t jupyterlab .
cd ..
docker run --name jupyterlab -p 8888:8888 -p 4200:4200 -v $(pwd):/app jupyterlab 

# To remove jupyterlab container
docker rm -f jupyterlab 