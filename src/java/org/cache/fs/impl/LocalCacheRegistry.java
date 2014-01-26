package org.cache.fs.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

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

  /**
     * newPath can come in as:
     *   - a.txt      ->> file
     *   - a/b/c      ->> file
     *   - a/b/c/     ->> directory
     *   - /b/c/a.txt ->> file
     */
  public String moveFile(String currPath, String newPath, Boolean mkfile) {
    _path = new File(FilenameUtils.getPath(newPath));

    if(mkfile && !_path.exists()) {
      _path.mkdirs();
    }

    if(isDirectory(newPath) ||
       StringUtils.isBlank(FilenameUtils.getName(newPath))) {
      // We have a directory
      _path = new File(_path, FilenameUtils.getName(currPath));
    } else {
      _path = new File(newPath);
    }

    try {
      (new File(currPath)).renameTo(_path);
    } catch(Exception e) {
      log.error("Could not rename file from "+currPath+" to "+newPath+"; error at "+e.getLocalizedMessage());
      return null;
    }

    return _path.toString();
  }

  /**
     * newPath can come in as:
     *   - a.txt      ->> file
     *   - a/b/c      ->> file
     *   - a/b/c/     ->> directory
     *   - /b/c/a.txt ->> file
     */
  public String moveDirectory(String currPath, String newPath, Boolean mkdir) {
    if(StringUtils.isBlank(FilenameUtils.getName(newPath))) {
      _path = new File(FilenameUtils.getPath(FilenameUtils.getFullPathNoEndSeparator(newPath)));
    } else {
      _path = new File(FilenameUtils.getPath(newPath));
    }

    if(mkdir && !_path.exists()) {
      _path.mkdirs();
    }

    _path = new File(newPath);

    try {
      (new File(currPath)).renameTo(_path);
    } catch(Exception e) {
      log.error("Could not rename file from "+currPath+" to "+newPath+"; error at "+e.getLocalizedMessage());
      return null;
    }

    return _path.toString();
  }

  public CachedFile registerCacheFile(String path, Boolean mkfile) {
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

  public CachedDirectory registerCacheDirectory(String path, Boolean mkdir) {
    _path = new File(path);

    if(mkdir && !_path.exists()) {
      _path.mkdirs();
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
      List<String> paths = new ArrayList<String>();

      for(String path : Arrays.asList(_path.list())) {
        paths.add(_path.getAbsolutePath().concat(path));
      }

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
