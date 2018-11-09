package com.dataextract.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.springframework.stereotype.Repository;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.api.services.cloudkms.v1.CloudKMSScopes;
import com.google.api.services.cloudkms.v1.model.DecryptRequest;
import com.google.api.services.cloudkms.v1.model.DecryptResponse;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

@Repository
public class AuthUtilsImpl implements AuthUtils {
	
	@Override
	public  Storage getStorageService(Credentials credentials) throws FileNotFoundException, IOException {

		

		
		Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
		return storage;

	}
	
	@Override
	  public  Credentials getCredentials(String jsonCredential) throws IOException {

		InputStream in = IOUtils.toInputStream(jsonCredential, "UTF-8");
		Credentials credentials = GoogleCredentials.fromStream(in);
		return credentials;
	}
  
@Override	
  public  CloudKMS getKmsService(GoogleCredential credential) throws FileNotFoundException, IOException {

		
	    HttpTransport transport = new NetHttpTransport();
	    JsonFactory jsonFactory = new JacksonFactory();

		
		
		 if (credential.createScopedRequired()) {
		      credential = credential.createScoped(CloudKMSScopes.all());
		    }

		return new CloudKMS.Builder(transport, jsonFactory, credential)
		        .setApplicationName("CloudKMS CryptFile")
		        .build();
		
	

	}
	@Override
	public  GoogleCredential getGoogleCredential(String jsonCredential) throws IOException {
	  
	  InputStream in = IOUtils.toInputStream(jsonCredential, "UTF-8");
	  GoogleCredential credential= GoogleCredential.fromStream(in);
	  return credential;
	  
	  
	}
	
	@Override
	  public  String fetchKey(String projectId, String locationId, String keyRingId,
		      String cryptoKeyId,  String serviceAccountName)
		      throws IOException {
		
		String source = "{\"type\": \"service_account\",\"project_id\": \"clouddatagrid\",\"private_key_id\": \"1ed0d0f2b2d8b245acf53a4d1242014678eaab6d\",\"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCXxvcVCiSa8In7\\n5lVteMf7Kmb1YOsCtT75Lgdh6eZlof9Y5+jvQNEK46yIJ+TiIBsZl1REtJpcKMYz\\nyeeFje3SrEVAZ4wx3thqbUvcNnRNJNU3rWguoFTy+KNi2txWHTR7b7HLW3Bupkg7\\nxRb7L4e+c2LmLn/ooHAwVU7yOiRlcJzKeXCXAV8IEUuDhCUwpvLwCH1rFkizJXWg\\n3bhC/k8nQEFSZ59UObh2l7nI0iuh9k8yOZTBnnL/u3+MBsiaMcTzKt+rdhF7RW3n\\nKj9AwRsz4B2QfGg0NM0RLohcD+Bf5P5O5gNJyi+EWRZb86pAny8oxRkdQTNnbw13\\nqKaLQpRTAgMBAAECggEAA+SETBkCqlShUqwWzyQfYOeL/3QZDZc6F72go9gSDX4+\\nTyNCMUyvb9Y6A73zhqx9ysYQ+7gevglaV/6wKO05xxhIUxK0y5ykXp+8p9e1Bjt7\\nxR317j2KIDcp8uxGZWZXWmkZcsE+x3UNT4x0/pk6o92lZjOVb+Zu1NaDRql2Fdc8\\n6UJ9ziIztTFJJFCc2wyq7KnYF9MpizEbLo9+zelxKosVJQ9vGf2HzowUHPrVUkD3\\nkamsIZnc66FZgbKoARYZUo51r1uTZoUrKHq6Q2pDjJQsXRlXVPw/g742G6ZAsGaM\\nO3SQSU9VD4RNWEXk10jBzvc5eLoBEFvdPSExwGaDwQKBgQDS0aRxK1XQmacRnbwx\\nGOaGk1tymwzrlbmIjZglnLwK9YM1xupaOKpn4Xv1Mo8Pq8uIxqvprNbYcb4NbWc8\\naTXX1TuaHUIJjcbrxVNLfrp7xF6nWlVJm3AIZ1RLGN/2/uT+rlrxhROdkZNEndW/\\nCp9kq1O228jHSl4k8eOPWXbnkwKBgQC4Tg/NmQFall7nlimoJNIru+1tqJNy1zYa\\n+E/mQP0vGitcmbFIccubmz5XmoJsIxqh31hKjdaGwYeXYF51AQR+nv7yoKsrrfJp\\nNSypxDrrdb3ij4zeuybt1ouLpRCJMzi2SqYRGjBvJObxASvJXzvPge5PJhwhu7P8\\ntDU0mbQYQQKBgQCs+gCQLrtpvjkbti2spz622v+kqF3QivhBd9SHv/N2ln2DSWQO\\nhQIk3BlGVaaWeCI3ZrO1tvBNhf4nrEWRhs37+uS9jhYaGq9F0pGGl0PRu4ziibcC\\n/zvMWWQLfix90CT6ZvsNwmAW4FRSb6Lq7n9cLUsx9WHnSzi13ZcWjpSGawKBgHAU\\nGIwg1a/u7bvLl32HFsA9wj8DWtdlhKOWCZCuSMlSErh3RlYVzYnNOHYxocp89n8l\\nwR1lb+X23qvxkL96ZO9TZRrLFgyz9UyBZ629hU8XBOg3/6SbDytnYukC8jFdEsnY\\nT3DrCjUsrbw1yBjnbwnbq746ILVq3iN4uzpHljuBAoGAZv1mf6L1s2CADKiEA+u+\\nFoM+ywQdGFCCiHzDGlsv+8/JpdsmqpJ1osvCYPYjbCccVuVq8TettP+5Fe10FZiw\\nTYHZoagixaD9o/hLUXZtpZQgZ/S70okig18nZuMGL/ypUh54QjSyvnnOMM41wQuC\\nwNeVh4XVm8TnZrT2ia+8+pA=\\n-----END PRIVATE KEY-----\\n\",\"client_email\": \"426072544020-compute@developer.gserviceaccount.com\",\"client_id\": \"107043515698469598083\",\"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\"token_uri\": \"https://oauth2.googleapis.com/token\",\"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/426072544020-compute%40developer.gserviceaccount.com\"}";
		Credentials Admincredentials=getCredentials(source);
		Storage storage=getStorageService(Admincredentials);
		GoogleCredential googleCredential=getGoogleCredential(source);
		CloudKMS kms=getKmsService(googleCredential);
		  Blob blob  = storage.get("key-store", "cypher-text/"+serviceAccountName+".txt");
		  byte[] content=blob.getContent();
		    String cryptoKeyName = String.format(
		        "projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
		        projectId, locationId, keyRingId, cryptoKeyId);

		    DecryptRequest request = new DecryptRequest().encodeCiphertext(content);
		    DecryptResponse response = kms.projects().locations().keyRings().cryptoKeys()
		        .decrypt(cryptoKeyName, request)
		        .execute();

		    String key=new String( response.decodePlaintext(),StandardCharsets.UTF_8);
		    return key;
		  }
	  
	
	

}
