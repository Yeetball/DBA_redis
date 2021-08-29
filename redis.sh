sudo apt update
sudo apt install redis-server
sudo cp /etc/redis/redis.conf /etc/redis.conf.default
redis-server
redis-cli
ping
exit
