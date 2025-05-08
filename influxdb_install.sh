curl --location -O \
https://download.influxdata.com/influxdb/releases/influxdb2-2.7.11_linux_amd64.tar.gz

# amd64
tar xvzf ./influxdb2-2.7.11_linux_amd64.tar.gz

# amd64
sudo cp ./influxdb2-2.7.11/usr/bin/influxd /usr/local/bin/

curl -L https://dl.influxdata.com/telegraf/releases/telegraf-1.34.3_linux_amd64.tar.gz \
     | tar xz

sudo cp ./telegraf-1.34.3/usr/bin/telegraf /usr/local/bin/