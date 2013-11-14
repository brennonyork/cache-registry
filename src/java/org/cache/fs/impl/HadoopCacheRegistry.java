package org.cache.fs.impl;

import java.io.InputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.log4j.Logger;

import org.cache.fs.CacheRegistry;

import org.cache.fs.sys.CachedFile;
import org.cache.fs.sys.CachedDirectory;

public class HadoopCacheRegistry extends CacheRegistry {
  static Logger log = Logger.getLogger(HadoopCacheRegistry.class);

  private static FileSystem _fs = null;
  private FileStatus _stat = null;

  public HadoopCacheRegistry() throws IOException {
    this(new Configuration());
  }

  public HadoopCacheRegistry(Configuration conf) throws IOException {
    super();

    try {
      _fs = FileSystem.get(conf);
    } catch(IOException e) {
      throw new IOException("Could not retrieve a FileSystem object given the configuration file; error at: "+e.getLocalizedMessage());
    }
  }

  public void close() {
    try {
      _fs.close();
    } catch(IOException e) {
      log.error("Could not close the FileSystem object");
    }

    return;
  }

  public Boolean isFile(String path) {
    try {
      _stat = _fs.getFileStatus(new Path(path));
      return !_stat.isDir();
    } catch(IOException e) {
      log.warn("Could not get FileStatus object from path "+path+" as file (does it exist?); error at: "+e.getLocalizedMessage());
    }

    return null;
  }

  public Boolean isDirectory(String path) {
    try {
      _stat = _fs.getFileStatus(new Path(path));
      return _stat.isDir();
    } catch(IOException e) {
      log.warn("Could not get FileStatus object from path "+path+" as directory (does it exist?); error at: "+e.getLocalizedMessage());
    }

    return null;
  }

  public CachedFile registerFile(String path) {
    try {
      return new HadoopFile(path, _fs);
    } catch(IOException e) {
      log.error("Could not register path "+path+" as file; error at: "+e.getLocalizedMessage());
    }

    return null;
  }

  public CachedDirectory registerDirectory(String path) {
    try {
      return new HadoopDirectory(path, _fs);
    } catch(IOException e) {
      log.error("Could not register path "+path+" as directory; error at: "+e.getLocalizedMessage());
    }

    return null;
  }

  public class HadoopFile implements CachedFile {
    private Path _path = null;
    private FileSystem _fs = null;
    private FileStatus _stat = null;
    private InputStream _fStream = null;
    private long _lastModTime;

    public HadoopFile(String path, FileSystem fs) throws IOException {
      _path = new Path(path);
      _fs = fs;

      try {
        _stat = _fs.getFileStatus(_path);
      } catch(IOException e) {
        throw new IOException("Could not get FileStatus object from path "+_path+"; error at: "+e.getLocalizedMessage());
      }

      if(_stat.isDir()) {
        throw new IOException("Attempted to create a CachedFile, but was given a directory path at "+path+".");
      }

      _lastModTime = _stat.getModificationTime();
    }

    public boolean isStale() {
      if(_lastModTime != _stat.getModificationTime()) {
        return true;
      } else {
        return false;
      }
    }

    public void setStaleFlag() {
      _lastModTime = _stat.getModificationTime();
    }

    public InputStream cachedInputStream() {
      if(_fStream == null) {
        log.warn("Attempting to retrieve a cached input stream when none was opened; will attempt to open.");

        try {
          _fStream = _fs.open(_path);
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
          _fStream = _fs.open(_path);
        } catch(IOException e) {
          log.error("Could not open file "+_path+"; error at: "+e.getLocalizedMessage());
          _fStream = null;
        }
      }

      return _fStream;
    }

    public InputStream open(int bufferSize) {
      if(_fStream != null) {
        log.warn("Attempting to open a previously opened file with name "+_path+"; should be using the cachedInputStream method.");
      } else {
        try {
          _fStream = _fs.open(_path, bufferSize);
        } catch(IOException e) {
          log.error("Could not open file "+_path+" with buffer size "+bufferSize+"; error at: "+e.getLocalizedMessage());
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

  public class HadoopDirectory implements CachedDirectory {
    private Path _path = null;
    private FileSystem _fs = null;
    private FileStatus _stat = null;
    private long _lastModTime;

    public HadoopDirectory(String path, FileSystem fs) throws IOException {
      _path = new Path(path);
      _fs = fs;

      try {
        _stat = _fs.getFileStatus(_path);
      } catch(IOException e) {
        throw new IOException("Could not get FileStatus object from file "+_path+"; error at: "+e.getLocalizedMessage());
      }

      if(!_stat.isDir()) {
        throw new IOException("Attempted to create a CachedDirectory, but was given a file path at "+path+".");
      }

      _lastModTime = _stat.getModificationTime();
    }

    public List<String> list() {
      List<String> paths = new ArrayList<String>();
      FileStatus[] files = null;

      try {
        files = _fs.listStatus(_path);
      } catch(IOException e) {
        log.error("Could not determine files for directory "+path"; error at: "+e.getLocalizedMessage());
      }

      for(FileStatus f : files) {
        paths.add(f.getPath().toString());
      }

      return paths;
    }

    public boolean isStale() {
      if(_lastModTime != _stat.getModificationTime()) {
        return true;
      } else {
        return false;
      }
    }

    public void setStaleFlag() {
      _lastModTime = _stat.getModificationTime();
    }
  }
}
