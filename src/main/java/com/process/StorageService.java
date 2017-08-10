package com.process;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.utils.Constants;
public class StorageService implements Constants {
	public static Storage storage = null;
	static Logger log = Logger.getLogger(StorageService.class.getName());

	public static void copyingFilefromPath(String bucketName,String archiveBucket,String fileName,String folder) throws IOException, GeneralSecurityException,Exception {
		if(storage == null){
			storage = storageInitiation();
		}
		log.info("Copying File from Storage - Start");
		try{
			// PLEASE NOTE THE USAGE OF setPrefi:@folder) TO FILTER ITEMS IN FOLDER
			com.google.api.services.storage.model.Objects objectsInFolder = storage.objects().list(bucketName).execute();
			for(StorageObject object : objectsInFolder.getItems()) {
				if(object.getName().contains(fileName) && object.getName().startsWith(fileName)){
					Storage.Objects.Copy request =
							storage
							.objects().copy(bucketName, object.getName(), archiveBucket,  object.getName(), object);
					StorageObject response = request.execute();
					//  storage.objects().delete(bucketName, object.getName()).execute();}
				}}
		}
		catch(GoogleJsonResponseException ex){
			log.info("Exception at copying file " + ex.getMessage());
		}
	}
	public static void deletingFileFromStorageFromProcessing(String bucketName,String fileName,String folder) throws IOException, GeneralSecurityException,Exception {

		if(storage == null){
			storage = storageInitiation();
		}

		log.info("Deleting File from Storage Processing - Start");
		try{

			// PLEASE NOTE THE USAGE OF setPrefi:@folder) TO FILTER ITEMS IN FOLDER
			com.google.api.services.storage.model.Objects objectsInFolder = storage.objects().list(bucketName).setPrefix(folder).execute();
			for(StorageObject object : objectsInFolder.getItems()) {
				if(object.getName().contains(fileName) && object.getName().contains(folder)){
					storage.objects().delete(bucketName, object.getName()).execute();
				}	
			}
		}
		catch(GoogleJsonResponseException ex){
			log.info("Exception at deleting file " + ex.getMessage());
		}



	}

	public static void deletingFileFromStorage(String bucketName,String fileName) throws IOException, GeneralSecurityException,Exception {

		if(storage == null){
			storage = storageInitiation();
		}

		log.info("Deleting File from Storage - Start");
		try{

			// PLEASE NOTE THE USAGE OF setPrefi:@folder) TO FILTER ITEMS IN FOLDER
			com.google.api.services.storage.model.Objects objectsInFolder = storage.objects().list(bucketName).execute();
			for(StorageObject object : objectsInFolder.getItems()) {
				if(object.getName().contains(fileName) && object.getName().startsWith(fileName)){
					log.info("Deleting File from Storage - Delete File");
					storage.objects().delete(bucketName, object.getName()).execute();
				}	
			}
		}
		catch(GoogleJsonResponseException ex){
			log.info("Exception at deleting file " + ex.getMessage());
		}
	}

	public static List<String> readFileFromStorage(String bucketName,String fileName,String folder) throws IOException, GeneralSecurityException,Exception 
	{
		List<String> fileNameList = new ArrayList<String>();

		if(storage == null){
			storage = storageInitiation();
		}
		try{

			// PLEASE NOTE THE USAGE OF setPrefi:@folder) TO FILTER ITEMS IN FOLDER
			com.google.api.services.storage.model.Objects objectsInFolder = storage.objects().list(bucketName).execute();
			if(objectsInFolder.getItems() != null){
				for(StorageObject object : objectsInFolder.getItems()) {
					if(object.getName().contains(fileName) && object.getName().startsWith(fileName)){
						if(fileNameList.size() < 10)
						fileNameList.add(object.getName());
					}
				}
			}
			log.info("Reading File from Storage - End");
		}catch(GoogleJsonResponseException ex){
			log.info("Exception at reading file " + ex.getMessage());
		}

		return fileNameList;
	}
	
	public static List<String> readFileFromStorageFolder(String bucketName,String fileName,String folder) throws IOException, GeneralSecurityException,Exception 
	{
		List<String> fileNameList = new ArrayList<String>();

		if(storage == null){
			storage = storageInitiation();
		}
		try{

			// PLEASE NOTE THE USAGE OF setPrefi:@folder) TO FILTER ITEMS IN FOLDER
			com.google.api.services.storage.model.Objects objectsInFolder = storage.objects().list(bucketName).execute();
			for(StorageObject object : objectsInFolder.getItems()) {
				if(object.getName().contains(fileName) && object.getName().startsWith(folder)){
					fileNameList.add(object.getName().replace(folder + "/", ""));
				}
			}

			log.info("Reading File from Storage - End");
		}catch(GoogleJsonResponseException ex){
			log.info("Exception at reading file " + ex.getMessage());
		}

		return fileNameList;
	}

	public static Storage storageInitiation()   {

		Storage storage1 = null;
		GoogleCredential credential;
		try {
			credential = GoogleCredential.getApplicationDefault();

			if (credential.createScopedRequired()) {
				credential =
						credential.createScoped(
								Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
			}

			HttpTransport httpTransport = null;
			try {
				httpTransport = GoogleNetHttpTransport.newTrustedTransport();
			} catch (GeneralSecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
			log.info("Storage Initiation");

			storage1 = new Storage(httpTransport, jsonFactory,
					new HttpRequestInitializer() {
				public void initialize(HttpRequest request)
						throws IOException {

					GoogleCredential credential = GoogleCredential.getApplicationDefault();
					if (credential.createScopedRequired()) {
						credential =
								credential.createScoped(
										Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
					}
					credential.initialize(request);
				}
			});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return storage1;
	}


}
