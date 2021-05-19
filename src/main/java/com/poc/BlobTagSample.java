package com.poc;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.TaggedBlobItem;
import com.azure.storage.common.StorageSharedKeyCredential;

import java.io.*;
import java.util.Hashtable;

public class BlobTagSample {

    // @TODO- to be part of property/env variables
    private static String AZURE_STORAGE_ACCOUNT="storage-acc-name";
    private static String AZURE_STORAGE_ACCESS_KEY="storage-access-key";

    //sample file creation
    static File createTempFile() throws IOException {

        File sampleFile = null;
        sampleFile = File.createTempFile("sampleFile", ".txt");
        System.out.println(">> Creating a sample file at: " + sampleFile.toString());
        Writer output = new BufferedWriter(new FileWriter(sampleFile));
        output.write("Hello Azure Storage blob quickstart.");
        output.close();
        return sampleFile;
    }

    /**
     * Main
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        // Retrieve the credentials and initialize SharedKeyCredentials
        String endpoint = "https://" + AZURE_STORAGE_ACCOUNT + ".blob.core.windows.net";
        String containerName = "storage-container-name";
        String blobName = "sampleBlob";

        // Create a SharedKeyCredential
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_ACCESS_KEY);

        // Create a blobServiceClient
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .buildClient();

        // create a blobcontainerclient
        BlobContainerClient blobContainerClient  = blobServiceClient.getBlobContainerClient(containerName);

        // create blob with blob index tag
        createBlobWithTags(blobContainerClient, blobName);

        // query container matching blob index tag
        queryContainerWithTags(blobServiceClient,containerName);
    }


    /**
     * Create Blob with Index Tag
     * @param blobContainerClient
     * @param blobName
     * @throws IOException
     */
    private static void createBlobWithTags(BlobContainerClient blobContainerClient, String blobName) throws IOException {

        // Create a BlobClient to run operations on Blobs
        BlobClient blobClient = blobContainerClient.getBlobClient(blobName);

        //upload blob
        blobClient.uploadFromFile(createTempFile().getPath());

        //set blob index tags
        blobClient.setTags(new Hashtable<String, String>() {
            {
                put("id", "1234");
                put("date", "2021-03-29");
            }
        });
    }

    /**
     * Query Container with tag
     * @param blobServiceClient
     * @param containerName
     */
    private static void queryContainerWithTags(BlobServiceClient blobServiceClient, String containerName) {

        // query constants
        String _ID = "1234";
        String _FROM = "2021-03-01";
        String _TO = "2021-03-25";

        // query for tag
        String containerScopedQuery = "@container = '" + containerName + "' AND \"id\" = '" + _ID +
                "' AND date >= '" + _FROM + "' AND date <= '" + _TO + "'";

        // query and results
        PagedIterable<TaggedBlobItem> iterable = blobServiceClient.findBlobsByTags(containerScopedQuery);
        for (TaggedBlobItem taggedBlobItem : iterable) {
            System.out.println(taggedBlobItem.getName());
        }
    }
}
