package com.utils;

// Library files.
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.Storage.Objects.Get;
import com.google.api.services.storage.model.ObjectAccessControl;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;

/**
 * Class for managing requests to Google Storage.
 * @author PXB8449
 *
 */
public class StorageManager {

  /**
   * The logger.
   */
  private static Logger log = Logger.getLogger(StorageManager.class.getName());
  
  /**
   * The Storage service.
   */
  private static Storage service = null;

  /**
   * Main entry class for testing.
   * @param args
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws InterruptedException {
    try {
      StorageManager.ungzip(
        "jda_qa",
        "FIXTURE",
        "uncompressed2",
        "FIXTURE/IX_SPC_PERFORMANCE.00_20161125.csv.gz");

    } catch (IOException e) {
      log.severe("Failed to test storage service " + e.getMessage());
      e.printStackTrace();
    } catch (GeneralSecurityException e) {
      log.severe("Failed to test storage service " + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * Unzip a gzip file in its provided folder.
   *
   * @param bucket - The bucket storing the zip file.
   * @param input - The input folder storing the zip file.
   * @param output - The output folder for writing the uncompressed data.
   * @param fname - The name of the zip file.
   *
   * @throws GeneralSecurityException 
   * @throws IOException 
   * @throws InterruptedException 
   */
  public static void ungzip(
      String bucket,
      String input,
      String output,
      String fname) throws IOException, GeneralSecurityException, InterruptedException {

    StorageManager.checkInitialization();

    log.info("Looking for the gzip file: " + fname);
    StorageObject obj = StorageManager.findObject(bucket, fname);
    if(obj == null) {
      return;
    }

    log.info("Grabbing the reference for the gzip file: " + fname);
    Get fileRef = StorageManager.getFileRef(bucket,obj);
    if(fileRef == null) {
      return;
    }

    log.info("Reading the gzip file: " + fname);
    List<ObjectAccessControl> acl = obj.getAcl();

    // If not in AppEngine, download the
    // whole thing in one request if possible.
    fileRef.getMediaHttpDownloader().setDirectDownloadEnabled(true);
    InputStream raw_is = fileRef.executeMediaAsInputStream();

    // Start the gzip stream.
    GZIPInputStream gis = new GZIPInputStream(raw_is);
    BufferedReader bufferedReader = new BufferedReader(
      new InputStreamReader(gis, "UTF-8"));

    // Partition the files every 100 K lines.
    String line;
    int lcount = 0;
    int fcount = 1;

    StringBuffer buf = new StringBuffer();
    while ((line = bufferedReader.readLine()) != null) {
      if(lcount == 100000) {
        log.info(String.format(
          "Creating file:  part-%d.txt", fcount));
        StorageManager.write(
          bucket,
          output,
          "fixture"+"-" + fcount,
          buf.toString().getBytes(),
          acl);
        lcount = 0;
        fcount++;
        buf = new StringBuffer();
      }

      buf.append(line);
      lcount++;
    }

    // Write the remaining data.
    log.info(String.format(
      "Creating file:  part-%d.txt", fcount));
    StorageManager.write(
      bucket,
      output,
      "fixture"+"-" + fcount,
      buf.toString().getBytes(),
      acl);
  }

  /**
   * Unzip a gzip file in its provided folder.
   *
   * @param bucket - The bucket storing the zip file.
   * @param input - The input folder storing the zip file.
   * @param output - The output folder for writing the uncompressed data.
   * @param fname - The name of the zip file.
   *
   * @throws GeneralSecurityException 
   * @throws IOException 
   * @throws InterruptedException 
   */
  public static void ungzipFile(
      String bucket,
      String input,
      String output,
      String fname) throws IOException, GeneralSecurityException, InterruptedException {

    StorageManager.checkInitialization();

    log.info("Looking for the gzip file: " + fname);
    StorageObject obj = StorageManager.findObject(bucket, fname);
    if(obj == null) {
      return;
    }

    log.info("Grabbing the file reference for the gzip file: " + fname);
    Get fileRef = StorageManager.getFileRef(bucket,obj);
    if(fileRef == null) {
      return;
    }
    
    log.info("Reading the gzip file: " + fname);    
    int timeout_ms = 100;
    int tryCount = 0;

    while(true) {
      try {
        
        List<ObjectAccessControl> acl = obj.getAcl();
        
        // If not in AppEngine, download the
        // whole thing in one request if possible.
        fileRef.getMediaHttpDownloader().setDirectDownloadEnabled(true);
        InputStream raw_is = fileRef.executeMediaAsInputStream();

        // Start the gzip stream.
        GZIPInputStream gis = new GZIPInputStream(raw_is);

        String writeFolder = output.isEmpty() ? "" : output + "/";
        StorageObject metadata = new StorageObject().
          setName(writeFolder + fname.replace("csv.gz", "csv")).
          setAcl(acl);
        InputStreamContent contentStream = new InputStreamContent(
            "text/plain", gis);

        log.info("Writing file: " + writeFolder + fname);
        StorageManager.service.
          objects().
          insert(bucket, metadata, contentStream).
          execute();
        break;

      } catch (IOException e) {
        
        Thread.currentThread();
        Thread.sleep(timeout_ms);
        timeout_ms *= 2;
        tryCount++;

        if(tryCount > 9) {
          String errorMessage = String.format(
            "\nFailed to unzip the gzip file \"%s\"" +
            "\nError message: \"%s\"", fname, e.getMessage());
          log.severe(errorMessage);
          throw e;
        }        
      }
    }
  }

  /**
   * Read the specified file from Google Cloud storage.
   *
   * @param bucket - The name of the bucket.
   * @param folder - The folder.
   * @param fname - The name of the file.
   * 
   * @return - The content read.
   * @throws IOException
   * @throws GeneralSecurityException 
 * @throws InterruptedException 
   */
  public static String readFile(
    String bucket,
    String folder,
    String fname) throws IOException, GeneralSecurityException, InterruptedException {

    StorageManager.checkInitialization();

    log.info("Looking for the file: " + fname);
    StorageObject obj = StorageManager.findObject(bucket, fname);
    if(obj == null) {
      return "";
    }

    log.info("Grabbing the file reference for the file: " + fname);
    Get fileRef = StorageManager.getFileRef(bucket,obj);  
    if(fileRef == null) {
       return "";
    }

    log.info("Reading the file: " + fname);
    int timeout_ms = 100;
    int tryCount = 0;

    while(true) {
      try {

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        // If not in AppEngine, download the
        // whole thing in one request if possible.
        fileRef.getMediaHttpDownloader().setDirectDownloadEnabled(true);
        fileRef.executeMediaAndDownloadTo(out);
        out.flush();
        out.close();

        String result = out.toString("UTF-8");
        if(result.startsWith("\uFEFF")) {
          result = result.substring(1);
        }

        return result;

      } catch(IOException e) {
        
        Thread.currentThread();
        Thread.sleep(timeout_ms);
        timeout_ms *= 2;
        tryCount++;

        if(tryCount > 9) {
          String errorMessage = String.format(
            "\nFailed to read the file \"%s\" from the folder \"%s\"" +
            "\nError message: \"%s\"", fname, folder, e.getMessage());
          log.severe(errorMessage);
          throw e;
        }
      }
    }
  }

  /**
   * Write data to a bucket in cloud storage.
   *
   * @param bucket
   * @param folder
   * @param fileName
   * @param data
   * @param acl
   *
   * @throws IOException
   * @throws InterruptedException 
   */
  private static void write(
    String bucket,
    String folder,
    String fileName,
    byte[] data,
    List<ObjectAccessControl> acl) throws IOException, InterruptedException {

    int timeout_ms = 100;
    int tryCount = 0;

    while(true) {
      try {
        String writeFolder = folder.isEmpty() ? "" : folder + "/";
        StorageObject metadata = new StorageObject().
          setName(writeFolder + fileName).
          setAcl(acl);

        InputStreamContent contentStream = new InputStreamContent(
            "text/plain", new ByteArrayInputStream(data));
        
        log.info("Writing file: " + writeFolder + fileName);
        StorageManager.service.
          objects().
          insert(bucket, metadata, contentStream).
          execute();
        
        break;
      } catch(IOException e) {

        Thread.currentThread();
        Thread.sleep(timeout_ms);
        timeout_ms *= 2;
        tryCount++;

        if(tryCount > 9) {
          String errorMessage = String.format(
            "\nFailed to write the file \"%s\"" +
            "\nError message: \"%s\"", fileName, e.getMessage());
          log.severe(errorMessage);
          throw e;
        }
      }
    }
  }

  /**
   * Helper function for returning a storage object from a list of objects.
   *
   * @param bucket - The bucket to search through.
   * @param name - The name of the object to find.
   * @return - The matching object in the list, or null if otherwise.
   *
   * @throws IOException 
   * @throws InterruptedException 
   */
  private static StorageObject findObject(String bucket, String name)
    throws IOException, InterruptedException {

    int timeout_ms = 100;
    int tryCount = 0;

    while(true) {
      try {
        Objects objectsInFolder = StorageManager.service.
          objects().
          list(bucket).
          setPrefix(name).
          execute();
        for (StorageObject object : objectsInFolder.getItems()) {
          if (object.getName().equals(name)) {
            return object;
          }
        }

        return null;
      } catch(IOException e) {

        Thread.currentThread();
        Thread.sleep(timeout_ms);
        timeout_ms *= 2;
        tryCount++;

        if(tryCount > 9) {
          String errorMessage = String.format(
            "\nFailed to find the file \"%s\"" +
            "\nError message: \"%s\"", name, e.getMessage());
          log.severe(errorMessage);
          throw e;
        }
      }
    }
  }

  /**
   * Helper function for fetching the file reference.
 * @throws InterruptedException 
 * @throws IOException 
   */
  private static Get getFileRef(String bucket,StorageObject obj)
    throws InterruptedException, IOException {

    int timeout_ms = 100;
    int tryCount = 0;

    while(true) {
      try {
        return StorageManager.service.objects().get(bucket, obj.getName());

      } catch(IOException e) {
        
        Thread.currentThread();
        Thread.sleep(timeout_ms);
        timeout_ms *= 2;
        tryCount++;

        if(tryCount > 9) {
          String errorMessage = String.format(
            "\nFailed to get the file reference for the file \"%s\"" +
            "\nError message: \"%s\"", obj.getName(), e.getMessage());
          log.severe(errorMessage);
          throw e;
        }
      }
    }
  }

  /**
   * Check if the manager is initialize, and
   * initialize it if not.
   *
   * @throws IOException 
   * @throws GeneralSecurityException 
   */
  private static void checkInitialization()
    throws IOException, GeneralSecurityException {

    if(StorageManager.service == null) {
      GoogleCredential credential = StorageManager.getCredentials();
      StorageManager.service = StorageManager.buildService(credential);
    }
  }

  /**
   * Return a reference to the Cloud Storage service.
   *
   * @param credential - The credentials to use for the service.
   * @return - The reference to the Cloud Storage service.
   * @throws IOException
   * @throws GeneralSecurityException
   */
  private static Storage buildService(
    final GoogleCredential credential)
      throws IOException, GeneralSecurityException {

    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

    Storage storage = new Storage(httpTransport, jsonFactory, new HttpRequestInitializer() {
      public void initialize(HttpRequest request) throws IOException {
        credential.initialize(request);
      }
    });

    return storage;
  }

  /**
   * Get credentials for the Cloud Storage service.
   *
   * @return - The credentials.
   * @throws IOException
   */
  private static GoogleCredential getCredentials()
    throws IOException {

    GoogleCredential credential = GoogleCredential.getApplicationDefault();

    if (credential.createScopedRequired()) {
      credential = credential
        .createScoped(
          Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }

    return credential;
  }
}

