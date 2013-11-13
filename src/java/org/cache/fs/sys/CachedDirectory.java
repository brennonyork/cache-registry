package org.cache.fs.sys;

import java.io.InputStream;

import java.util.List;

public interface CachedDirectory {
  boolean isStale();
  List<String> list();
  void setStaleFlag();
}
