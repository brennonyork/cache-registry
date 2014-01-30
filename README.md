# cache-registry

A simplified java library to act as an in-memory cache for filesystem objects. Useful for distributed applications such as Storm.

## Usage

Currently there are two core implementations, the HadoopCacheRegistry and the LocalCacheRegistry, for Hadoop and the local filesystem respectively.

## Caveats

To work with the two divergent branches of Hadoop (1.x and 2.x) I've chosen, for the time being, to leave the Cache Registry 2.x version mapping to Hadoop 1.x and Cache Registry 3.x versions to map to Hadoop 2.x. As always, let me know if you have any issues!

## License

Copyright © 2013 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
