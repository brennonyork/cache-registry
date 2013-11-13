package org.cache.fs.sys;

import java.io.InputStream;

public interface CachedFile {
  boolean isStale();
  void setStaleFlag();
  InputStream open();
  void close();
  InputStream cachedInputStream();
}
