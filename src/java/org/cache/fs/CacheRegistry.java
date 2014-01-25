package org.cache.fs;

import java.lang.StringBuilder;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;

import java.io.InputStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import org.cache.fs.sys.CachedFile;
import org.cache.fs.sys.CachedDirectory;

/**
 * Creates a fail-open system for managing file system objects.
 *
 * @author Brennon York
 */
public abstract class CacheRegistry {
  static Logger log = Logger.getLogger(CacheRegistry.class);

  private Map<String,CachedFile> _fileRegistry = null;
  private Map<String,CachedDirectory> _directoryRegistry = null;
  private Boolean _mkpath = false;

  public CacheRegistry() throws OutOfMemoryError {
    _fileRegistry = new HashMap<String,CachedFile>();
    _directoryRegistry = new HashMap<String,CachedDirectory>();

    if(_fileRegistry == null) {
      throw new OutOfMemoryError("Could not initialize file registry");
    }

    if(_directoryRegistry == null) {
      throw new OutOfMemoryError("Could not initialize directory registry");
    }
  }

  /**
     * Register a new file with the Registry
     */
  public String registerFile(String filePath) {
    return registerFile(filePath, _mkpath);
  }

  /**
     * Register a new file with the Registry and create the path if set
     */
  public String registerFile(String path, Boolean mkfile) {
    if(StringUtils.isBlank(path)) {
      logBadPath("registerFile");
      return null;
    }

    if(isFile(path) && _fileRegistry.containsKey(path)) {
      return path;
    } else if(assertRegister(path, _fileRegistry, registerCacheFile(path, mkfile)) != null) {
      _fileRegistry.get(path).open();
      return path;
    }

    return null;
  }

  /**
     * Register a new directory with the Registry
     */
  public String registerDirectory(String dirPath) {
    return registerDirectory(dirPath, _mkpath);
  }

  /**
     * Register a new directory with the Registry and create the path if set
     */
  public String registerDirectory(String path, Boolean mkdir) {
    if(StringUtils.isBlank(path)) {
      logBadPath("registerDirectory");
      return null;
    }

    if(isDirectory(path) && _directoryRegistry.containsKey(path)) {
      return path;
    } else {
      return assertRegister(path, _directoryRegistry, registerCacheDirectory(path, mkdir));
    }
  }

  /**
     * Assert all assignments into the registry with the given path are complete
     */
  private <T> String assertRegister(String path, Map<String,T> registry, T registeredPath) {
    if(registry == null) {
      log.error("Found uninitialized registry"+regErrStr(path));
      return null;
    }

    if(registeredPath == null) {
      log.warn("Could not create a register object for path provided"+regErrStr(path));
      return null;
    }

    registry.put(path, registeredPath);

    log.debug("Successfully registered path "+path+".");

    return path;
  }

  /**
     * Return the list of files for the given path provided
     *
     * @return If a directory is given, then all objects under the directory will be returned
     *         within the array. If a file is given, then the path provided is returned as an
     *         array of a single element.
     */
  public List<String> list(String path) {
    if(StringUtils.isBlank(path)) {
      logBadPath("listFiles");
      return null;
    }

    if(isFile(path)) {
      return Arrays.asList(new String[]{path});
    } else if(isDirectory(path)) {
      CachedDirectory rd = null;

      if(_directoryRegistry.containsKey(path)) {
        rd = _directoryRegistry.get(path);

        if(rd.isStale()) {
          rd.setStaleFlag();
          return rd.list();
        } else {
          return rd.list();
        }
      } else {
        logUnregistered("directory", path);

        if(assertRegister(path, _directoryRegistry, registerCacheDirectory(path, _mkpath)) != null) {
          return _directoryRegistry.get(path).list();
        } else {
          logRegistrationFailed("directory", path);
          return null;
        }
      }
    } else {
      logBadType(path);
    }

    return null;
  }

  /**
     * Move a file into the new path
     *
     * @param currPath a file path, cannot be a directory
     * @param newPath the file or directory with which to be renamed or placed into
     *
     * @return the newPath if successful, else null for any errors
     */
  public String move(String currPath, String newPath) {
    String actNewPath = null;

    if(StringUtils.isBlank(currPath) ||
       StringUtils.isBlank(newPath)) {
      logBadPath("move");
      return null;
    }

    if(isFile(currPath)) {
      if(_fileRegistry.containsKey(currPath)) {
        unregister(currPath);
      }

      actNewPath = moveFile(currPath, newPath, _mkpath);

      if(actNewPath != null) {
        if(assertRegister(actNewPath, _fileRegistry, registerCacheFile(actNewPath, _mkpath)) != null) {
          return actNewPath;
        } else {
          logRegistrationFailed("file", newPath);
          return null;
        }
      } else {
        logBadMove("file",currPath,newPath);
        return null;
      }
    } else if(isDirectory(currPath)) {
      if(_directoryRegistry.containsKey(currPath)) {
        unregister(currPath);
      }

      actNewPath = moveDirectory(currPath, newPath, _mkpath);

      if(actNewPath != null) {
        if(assertRegister(actNewPath, _directoryRegistry, registerCacheDirectory(actNewPath, _mkpath)) != null) {
          return actNewPath;
        } else {
          logRegistrationFailed("directory", newPath);
          return null;
        }
      } else {
        logBadMove("directory",currPath,newPath);
        return null;
      }
    } else {
      logBadType(currPath);
    }

    return null;
  }

  /**
     * Check if any changes have occurred to the path specified.
     *
     * This is useful for applications leveraging a CacheRegistry and using it to load or
     * serialize data into Java objects.
     */
  public Boolean isStale(String path) {
    if(StringUtils.isBlank(path)) {
      logBadPath("isStale");
      return null;
    }

    if(isFile(path)) {
      if(_fileRegistry.containsKey(path)) {
        return _fileRegistry.get(path).isStale();
      } else {
        logUnregistered("file", path);

        if(assertRegister(path, _fileRegistry, registerCacheFile(path, _mkpath)) != null) {
          return _fileRegistry.get(path).isStale();
        } else {
          logRegistrationFailed("file", path);
          return null;
        }
      }
    } else if(isDirectory(path)) {
      if(_directoryRegistry.containsKey(path)) {
        return _directoryRegistry.get(path).isStale();
      } else {
        logUnregistered("directory", path);

        if(assertRegister(path, _directoryRegistry, registerCacheDirectory(path, _mkpath)) != null) {
          return _directoryRegistry.get(path).isStale();
        } else {
          logRegistrationFailed("directory", path);
          return null;
        }
      }
    } else {
      logBadType(path);
    }

    return null;
  }

  /**
     * Return the native file stream object for the file at the given path
     *
     * This will return a cached InputStream if one was already opened and is, therefore,
     * performance-safe for multiple calls to this method. If the cache is stale and the file
     * has since been modified the system will close the old file descriptor, set the stale flag
     * to the current value, and return the newly opened file.
     *
     * @return A new instance of the InputStream or the previously cached instance if one was
     *         already opened.
     */
  public InputStream getStream(String path) {
    if(StringUtils.isBlank(path)) {
      logBadPath("getStream");
      return null;
    }

    if(isFile(path)) {
      CachedFile rf = null;

      if(_fileRegistry.containsKey(path)) {
        rf = _fileRegistry.get(path);

        if(rf.isStale()) {
          rf.close();
          rf.setStaleFlag();
          return rf.open();
        } else {
          return rf.cachedInputStream();
        }
      } else {
        logUnregistered("file", path);

        if(assertRegister(path, _fileRegistry, registerCacheFile(path, _mkpath)) != null) {
          return _fileRegistry.get(path).open();
        } else {
          logRegistrationFailed("file", path);
          return null;
        }
      }
    } else {
      log.error("Could not determine file as path: "+path+"; cannot return InputStream.");
    }

    return null;
  }

  /**
     * Set default behavior for any registry method called within the system
     */
  public void setRegistryCreation(Boolean mkpath) {
    _mkpath = mkpath;
  }

  /**
     * Remove the path from the monitored list and close it if a file
     */
  public void unregister(String path) {
    if(StringUtils.isBlank(path)) {
      logBadPath("unregister");
      return;
    }

    if(_fileRegistry.containsKey(path)) {
      _fileRegistry.get(path).close();
      _fileRegistry.remove(path);
    } else if(_directoryRegistry.containsKey(path)) {
      _directoryRegistry.remove(path);
    } else {
      logBadType(path);
    }

    return;
  }

  /**
     * Closes all files associated with the CacheRegistry instance and clears all registries
     */
  public void destroy() {
    for(Entry<String,CachedFile> kv : _fileRegistry.entrySet()) {
      kv.getValue().close();
    }

    _fileRegistry.clear();
    _directoryRegistry.clear();

    close();

    return;
  }

  /**
     * Generate a common registry error string given the path to concatenate onto all log messages
     */
  private String regErrStr(String path) {
    return new StringBuilder("; cannot register ").append(path).append(".").toString();
  }

  /**
     * Generate a common path error given the method for all log messages
     */
  private void logBadPath(String method) {
    log.warn("Provided path of either only whitespace, empty, or null into method "+method+"().");
  }

  /**
     * Generate a common log when path cannot be determined as either a file or directory
     */
  private void logBadType(String path) {
    log.warn("Cannot determine path as either file or directory"+regErrStr(path));
  }

  /**
     * Generate a common log when the move of a file or directory fails
     */
  private void logBadMove(String regType, String currPath, String newPath) {
    log.warn("Cannot move "+regType+" from "+currPath+" to "+newPath+".");
  }

  /**
     * Generate a common unregistered lookup error string given the path and registry type for
     * all log messages
     */
  private void logUnregistered(String regType, String path) {
    log.warn("Found unregistered "+regType+" as path: "+path+"; attempting to register.");
  }

  /**
     * Generate a common registration error when reattempting to a registration given the path and
     * type of registry
     */
  private void logRegistrationFailed(String regType, String path) {
    log.error("Attempted to register path as "+regType+", but failed"+regErrStr(path));
  }

  /**
     * Close any resources instantiated with the CacheRegistry object
     */
  protected abstract void close();

  /**
     * @return Either the boolean value of true or false if the path is a file, otherwise null
     *         if an error has occurred.
     */
  protected abstract Boolean isFile(String path);

  /**
     * @return Either the boolean value of true or false if the path is a directory, otherwise null
     *         if an error has occurred.
     */
  protected abstract Boolean isDirectory(String path);

  /**
     * Moves a file from the current path into the new path provided
     *
     * @param currPath current path of file
     * @param newPath new path for file
     * @param mkfile flag whether to create the path to the file or not
     * @return the newPath if successful, else null
     */
  protected abstract String moveFile(String currPath, String newPath, Boolean mkfile);

  /**
     * Moves a directory from the current path into the new path provided
     *
     * @param currPath current path of directory
     * @param newPath new path for the directory
     * @param mkfile flag whether to create the path to the directory or not
     * @return the newPath if successful, else null
     */
  protected abstract String moveDirectory(String currPath, String newPath, Boolean mkdir);

  /**
     * @param mkfile boolean value in whether the file should be created if it doesn't already exist
     * @return A CachedFile to register the file with the system, else null if an error occurred.
     */
  protected abstract CachedFile registerCacheFile(String path, Boolean mkfile);

  /**
     * @param mkdir boolean value in whether the directory should be created if it doesn't already exist
     * @return A CachedDirectory to register the directory with the system, else null if an error occurred.
     */
  protected abstract CachedDirectory registerCacheDirectory(String path, Boolean mkdir);
}
