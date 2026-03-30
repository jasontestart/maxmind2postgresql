# maxmind2postgresql
I have a cron job that keeps a local copy of Maxmind's GeoLite2 databases current. I basically scripted what's here: [https://dev.maxmind.com/geoip/importing-databases/postgresql/](https://dev.maxmind.com/geoip/importing-databases/postgresql/).

This script requires a `GeoIP.conf` file, generated from a Maxmind account, in the current directory. See [https://dev.maxmind.com/geoip/updating-databases](https://dev.maxmind.com/geoip/updating-databases).
