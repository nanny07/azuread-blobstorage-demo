using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.DataMovement;

namespace BlobStorageDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            //keep it secret: create your StorageConnectionString.txt file before run this example
            var storageConnectionString = File.ReadAllText("StorageConnectionString.txt");

            //get the account in which are stored all the blob containers/file shares/queues/tables
            var account = CloudStorageAccount.Parse(storageConnectionString);

            //create the client to interact with blob
            var blobClient = account.CreateCloudBlobClient();

            /*
             * get the list of all my blob containers and print theirs name
             * Params:
             *  - prefix: limit the search only to blob containers that starts with "d"
             *  - detailsIncluded: if you want some details for your containers you have to ask for it (or retreive later)
             */
            var enumerableBlobContainers = blobClient.ListContainers(prefix:"d", detailsIncluded: ContainerListingDetails.Metadata);
            foreach(var bc in enumerableBlobContainers)
            {
                Console.WriteLine(bc.Name);
                //if I haven't specifyied the detailsIncluded, I need to call this to retrive all the attributes of my container
                //bc.FetchAttributes();
                foreach (var metadataItem in bc.Metadata)
                {
                    Console.WriteLine("\t\tMetadata key: " + metadataItem.Key);
                    Console.WriteLine("\t\tMetadata value: " + metadataItem.Value);
                }
            }

            //get a specific blob container: in this case I want the one named "demo"
            var blobContainer = blobClient.GetContainerReference("demo");

            /*
             * retrive the list of all blob files in the container. The opt params are likely the same of the ListCatainers
             * prefix, BlobListingDetails ecc...
             * There are cases in which you want to request list of blob files in segments: we need a BlobContinuationToken to check
             * whenever it's NULL
             */
            BlobContinuationToken continuationToken = null;
            var segmentSize = 3;
            do
            {
                var resultSegment = blobContainer.ListBlobsSegmented(
                      prefix: string.Empty
                    , useFlatBlobListing: true //A flat listing operation returns only blobs, not virtual directories.
                    , blobListingDetails: BlobListingDetails.None //You can specify if you want the list off all snapshots
                    , maxResults: segmentSize //how many blob per segment
                    , currentToken: continuationToken //the token to check if I have more results
                    , operationContext: null
                    , options: null);

                foreach (var blobItem in resultSegment.Results)
                {
                    var blob = blobItem as CloudBlob;
                    Console.WriteLine("Blob name: {0}", blob.Name);
                }

                // before continue in the loop, get the ContinuationToken
                continuationToken = resultSegment.ContinuationToken;
            } while (!(continuationToken is null));

            //get the block reference from the blob container
            var blockBlobRef = blobContainer.GetBlockBlobReference("test-file.txt");
            if (!blockBlobRef.Exists(options: null, operationContext: null))
            {
                //create a file
                File.WriteAllText("test-file.txt", "test content");

                //upload the file. By default the snapshot is not created (every snapshot will cost you)
                blockBlobRef.UploadFromFile("test-file.txt");

                //download the file
                blockBlobRef.DownloadToFile("test-file-from-azure.txt", FileMode.Create);
            }

            //create the snapshot: by default the snapshot is not created (every snapshot will cost you)
            blockBlobRef.Snapshot();

            //create a new file
            File.WriteAllText("test-file-modified.txt", "test content modified");
            //the reference is still the same from the blob named "test-file.txt"
            blockBlobRef.UploadFromFile("test-file-modified.txt");

            //retrive the list of snapshots
            var snapshots = blobContainer.ListBlobs(prefix: "test-file.txt", blobListingDetails: BlobListingDetails.Snapshots, useFlatBlobListing: true)
                .Cast<CloudBlockBlob>()
                .Where(x => x.IsSnapshot && x.Name == "test-file.txt") //whe can use the prefix
                .ToList();

            foreach (var snap in snapshots)
            {
                Console.WriteLine($"Snapshot of {snap.Name} taken at: {snap.SnapshotTime.Value.ToLocalTime()}");
            }

            //To restore a snapshot you just have to copy the snapshot to the blob
            var blobToRestore = blobContainer.GetBlockBlobReference("test-file.txt");
            var snapshotToRestore = snapshots.First();
            blobToRestore.StartCopy(snapshotToRestore);

            // deletes blob and snapshots: you can't delete a blob without deleting its snapshots
            blockBlobRef.DeleteIfExists(DeleteSnapshotsOption.IncludeSnapshots);

            //delete onyl snapshots
            blockBlobRef.DeleteIfExists(DeleteSnapshotsOption.DeleteSnapshotsOnly);

            //Getting Shared Access Signature: key valid only for the single blob file
            //Read more at: https://docs.microsoft.com/it-it/rest/api/storageservices/create-account-sas?redirectedfrom=MSDN

            //TODO
        }
    }
}
