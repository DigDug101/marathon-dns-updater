# marathon-lb-dns-updater
Monitors marathon-lb installations and ensures DNS records are up to date.

## Usage

```
usage: marathon_lb_dns_updater [OPTIONS]

OPTIONS:
  -app-id string
    	Marathon app id of marathon-lb service (default "marathon-lb")
  -hosted-zone-id string
    	Route53 Hosted Zone
  -interval int
    	Update interval (default 360)
  -marathon-host string
    	HTTP endpoint of Marathon service (default "http://marathon.mesos:8080")
  -record-set string
    	Record set to update (default "marathon-lb.ads.reddit.internal")
```
