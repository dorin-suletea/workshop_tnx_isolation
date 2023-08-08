# Workshop_tnx_isolation


## Setup Docker
* Install docker https://docs.docker.com/engine/install/ubuntu/
* Make sure docker service is running `service docker status` if not start it `service docker start`

## Create a MySql container
```
docker stop mysql
docker rm mysql
docker run --name mysql -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=dsu --restart unless-stopped -v mysql:/var/lib/mysql mysql:8.0.33-debian
```
Make sure it's up `docker ps -a`

## Create user and database
* sh into the docker instance `docker exec -it mysql sh`
* Run mysql cli while in the container `mysql -h localhost -u root --password=dsu`
* While in th mysql cli
```
CREATE USER 'usr_dsu' IDENTIFIED WITH mysql_native_password BY 'password';
GRANT ALL ON *.* to 'usr_dsu'@'%';
FLUSH PRIVILEGES;
CREATE DATABASE my_playground;
```
Type `exit` to quit mysql cli, type `exit` again to exit the docker shell. 
**You're done.** 
Start running java code.
