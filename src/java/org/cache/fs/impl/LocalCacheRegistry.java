package org.cache.fs.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import org.cache.fs.CacheRegistry;

import org.cache.fs.sys.CachedFile;
import org.cache.fs.sys.CachedDirectory;

public class LocalCacheRegistry extends CacheRegistry {
  static Logger log = Logger.getLogger(LocalCacheRegistry.class);

  private File _path = null;

  public LocalCacheRegistry() { }

  public void close() {
    return;
  }

  public Boolean isFile(String path) {
    return new File(path).isFile();
  }

  public Boolean isDirectory(String path) {
    return new File(path).isDirectory();
  }

  public CachedFile registerFile(String path, Boolean mkfile) {
    _path = new File(path);

    if(mkfile) {
      if(!_path.exists()) {
        try {
          _path.createNewFile();
        } catch(IOException e) {
          log.error("Could not create file as path "+path+"; error at "+e.getLocalizedMessage());
          return null;
        }
      }

      return registerFile(_path);
    } else {
      return registerFile(_path);
    }
  }

  private CachedFile registerFile(File path) {
    try {
      return new LocalFile(path);
    } catch(IOException e) {
      log.error("Could not register path "+path+" as file; error at: "+e.getLocalizedMessage());
    }

    return null;
  }

  public CachedDirectory registerDirectory(String path, Boolean mkdir) {
    _path = new File(path);

    if(mkdir && !_path.exists()) {
      _path.mkdir();
    }

    return registerDirectory(_path);
  }

  private CachedDirectory registerDirectory(File path) {
    try {
      return new LocalDirectory(path);
    } catch(IOException e) {
      log.error("Could not register path "+path+" as directory; error at: "+e.getLocalizedMessage());
    }

    return null;
  }

  public class LocalFile implements CachedFile {
    private File _path = null;
    private InputStream _fStream = null;
    private long _lastModTime;

    public LocalFile(File path) throws IOException {
      _path = path;

      if(!_path.isFile()) {
        throw new IOException("Attempted to create a CachedFile, but was given a directory path at "+path+".");
      }

      _lastModTime = _path.lastModified();
    }

    public boolean isStale() {
      if(_lastModTime != _path.lastModified()) {
        return true;
      } else {
        return false;
      }
    }

    public void setStaleFlag() {
      _lastModTime = _path.lastModified();
    }

    public InputStream cachedInputStream() {
      if(_fStream == null) {
        log.warn("Attempting to retrieve a cached input stream when none was opened; will attempt to open.");

        try {
          _fStream = new FileInputStream(_path);
        } catch(IOException e) {
          log.error("Could not open file "+_path+"; error at: "+e.getLocalizedMessage());
          _fStream = null;
        }
      }

      return _fStream;
    }

    public InputStream open() {
      if(_fStream != null) {
        log.warn("Attempting to open a previously opened file with name "+_path+"; should be using the cachedInputStream method.");
      } else {
        try {
          _fStream = new FileInputStream(_path);
        } catch(IOException e) {
          log.error("Could not open file "+_path+"; error at: "+e.getLocalizedMessage());
          _fStream = null;
        }
      }

      return _fStream;
    }

    public void close() {
      if(_fStream == null) {
        log.warn("Attempting to close a nonexistent file descriptor with name "+_path);
      } else {
        try {
          _fStream.close();
        } catch(IOException e) {
          log.error("Could not close file "+_path+"; error at: "+e.getLocalizedMessage());
        }

        _fStream = null;
      }
    }
  }

  public class LocalDirectory implements CachedDirectory {
    private File _path = null;
    private long _lastModTime;

    public LocalDirectory(File path) throws IOException {
      _path = path;

      if(!_path.isDirectory()) {
        throw new IOException("Attempted to create a CachedDirectory, but was given a file path at "+path+".");
      }

      _lastModTime = _path.lastModified();
    }

    public List<String> list() {
      List<String> paths = new ArrayList<String>(Arrays.asList(_path.list()));

      return paths;
    }

    public boolean isStale() {
      if(_lastModTime != _path.lastModified()) {
        return true;
      } else {
        return false;
      }
    }

    public void setStaleFlag() {
      _lastModTime = _path.lastModified();
    }
  }
}
