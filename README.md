# redigoring

`redigoring` is consistent hashing implementation for redigo [redis pool](https://godoc.org/github.com/gomodule/redigo/redis#Pool), used to distribute keys across multiple Redis servers using consistent hashing algorithm.


## Features
- distributes the keys using consistent hashing
- auto disable/enable pool based on the health check result

## References
- [Scaling Memcache at Facebook](https://www.usenix.org/system/files/conference/nsdi13/nsdi13-final170_update.pdf)
- [Consistent Hashing](https://en.wikipedia.org/wiki/Consistent_hashing)