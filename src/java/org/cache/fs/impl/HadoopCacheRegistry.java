package org.cache.fs.impl;

import java.io.InputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

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
  private Path _path = null;
	private static final String HADOOP_COPYING_SUFFIX = "_COPYING_";

  public HadoopCacheRegistry() throws IOException {
    this(new Configuration());
  }

  public HadoopCacheRegistry(Configuration conf) throws IOException {
    super();
		_fs = FileSystem.get(conf);
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
      return _fs.isFile(new Path(path));
    } catch(IOException e) {
      log.warn("Could not determine file from path "+path+" (does it exist?); error at: "+e.getLocalizedMessage());
      return false;
    }
  }

  public Boolean isDirectory(String path) {
    try {
      return _fs.getFileStatus(new Path(path)).isDirectory();
    } catch(IOException e) {
      log.warn("Could not get FileStatus object from path "+path+" as directory (does it exist?); error at: "+e.getLocalizedMessage());
      return false;
    }
  }

  /**
	 * newPath can come in as:
	 *   - a.txt      ->> file
	 *   - a/b/c      ->> if present and directory, directory; else file
	 *   - a/b/c/     ->> directory
	 *   - /b/c/a.txt ->> file
	 */
  public String moveFile(String currPath, String newPath, Boolean mkfile) {
    _path = new Path(FilenameUtils.getFullPath(newPath));

    if(mkfile) {
      Boolean exists = null;

      try {
        exists = _fs.exists(_path);
      } catch(IOException e) {
        log.error("Could not determine if path "+newPath+" exists; error at: "+e.getLocalizedMessage());
        return null;
      }

      try {
        _fs.mkdirs(_path);
      } catch(IOException e) {
        log.error("Could not create file as path "+newPath+"; error at "+e.getLocalizedMessage());
        return null;
      }
    }

    if(isDirectory(newPath) ||
       StringUtils.isBlank(FilenameUtils.getName(newPath))) {
      // We have a directory
      _path = new Path(_path, FilenameUtils.getName(currPath));
    } else {
      _path = new Path(newPath);
    }

    try {
      _fs.rename(new Path(currPath), _path);
    } catch(IOException e) {
      log.error("Could not rename file from "+currPath+" to "+_path.toString()+"; error at "+e.getLocalizedMessage());
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
      _path = new Path(FilenameUtils.getFullPath(FilenameUtils.getFullPathNoEndSeparator(newPath)));
    } else {
      _path = new Path(FilenameUtils.getFullPath(newPath));
    }

    if(mkdir) {
      Boolean exists = null;

      try {
        exists = _fs.exists(_path);
      } catch(IOException e) {
        log.error("Could not determine if path "+_path.toString()+" exists; error at: "+e.getLocalizedMessage());
        return null;
      }

      try {
        _fs.mkdirs(_path);
      } catch(IOException e) {
        log.error("Could not create file as path "+_path.toString()+"; error at "+e.getLocalizedMessage());
        return null;
      }
    }

    _path = new Path(newPath);

    try {
      _fs.rename(new Path(currPath), _path);
    } catch(IOException e) {
      log.error("Could not rename file from "+currPath+" to "+newPath+"; error at "+e.getLocalizedMessage());
      return null;
    }

    return _path.toString();
  }

  public CachedFile registerCacheFile(String path, Boolean mkfile) {
    _path = new Path(path);

    if(mkfile) {
      Boolean exists = null;

      try {
        exists = _fs.exists(_path);
      } catch(IOException e) {
        log.error("Could not determine if path "+path+" exists; error at: "+e.getLocalizedMessage());
        return null;
      }

      if(!exists) {
        try {
          _fs.createNewFile(_path);
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

  private CachedFile registerFile(Path path) {
    try {
      return new HadoopFile(path, _fs);
    } catch(IOException e) {
      log.error("Could not register path "+path+" as file; error at: "+e.getLocalizedMessage());
    }

    return null;
  }

  public CachedDirectory registerCacheDirectory(String path, Boolean mkdir) {
    _path = new Path(path);

    if(mkdir) {
      Boolean exists = null;

      try {
        exists = _fs.exists(_path);
      } catch(IOException e) {
        log.error("Could not determine if path "+path+" exists; error at: "+e.getLocalizedMessage());
        return null;
      }

      if(!exists) {
        try {
          _fs.mkdirs(_path);
        } catch(IOException e) {
          log.error("Could not create directory as path "+path+"; error at "+e.getLocalizedMessage());
          return null;
        }
      }

      return registerDirectory(_path);
    } else {
      return registerDirectory(_path);
    }
  }

  private CachedDirectory registerDirectory(Path path) {
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

    public HadoopFile(Path path, FileSystem fs) throws IOException {
      _path = path;
      _fs = fs;

      try {
        _stat = _fs.getFileStatus(_path);
      } catch(IOException e) {
        throw new IOException("Could not get FileStatus object from path "+_path+"; error at: "+e.getLocalizedMessage());
      }

      if(_stat.isDirectory()) {
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

    public HadoopDirectory(Path path, FileSystem fs) throws IOException {
      _path = path;
      _fs = fs;

      try {
        _stat = _fs.getFileStatus(_path);
      } catch(IOException e) {
				log.error("Could not get FileStatus object from file "+_path);
				throw e;
      }

      if(!_stat.isDirectory()) {
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
        log.error("Could not determine files for directory "+_path+"; error at: "+e.getLocalizedMessage());
      }

      for(FileStatus f : files) {
				String pathString = f.getPath().toString();
				/** only show full files */
				if(!pathString.endsWith(HADOOP_COPYING_SUFFIX)){
					paths.add(f.getPath().toString());
				}
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
