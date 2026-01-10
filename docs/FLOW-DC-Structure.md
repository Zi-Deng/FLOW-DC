# FLOW-DC Structure

## Preprocessing Step

- Figure out the dataset you want and what items to include in the dataset

- Generate a dataset manifest (usually a parquet) where each row is the location (usually a URL) of an item in the dataset. The manifest can also contain the class labels for each item in the row as well as an additional column or other metadata columns as the user sees fit.

  - For example, for iNaturalist, this involves creating a SQL database using the metadata files provided from iNaturalist Open Data; then configuring and querying this database to output just the images you want to download and subsequently storing that into a parquet file.

## Step 1: Manager Machine Processing and Instantiation

- Manager receives the dataset manifest

- Manager partitions the dataset into fixed length chunks (also parquets) using the manifest parquet. These fixed length chunks serve as the input to the worker machine downloading scripts. The length of the chunks is a hyperparameter decided by the user (for Biotrove it was 100,000 rows per parquet).

  - There are two ways we can do the grouping: standard or greedy grouping by host

    - **Standard:** We partition the dataset into equal sized chunks and each item in each chunk is selected randomly. If your entire dataset is from one host you can use this option to be more computationally efficient.

    - **Greedy grouping by host:** We first determine what the host server from the URL of each item in the dataset is, then we create an additional column onto the dataset manifest which is the host server of the item. We then perform greedy grouping on the host column such that as many items of the same host are grouped together as possible in the partition.

- Once the partitions are created, the manager machine instantiates the TaskVine workflow script that specifies what the workers receive as inputs/files and what the workers send back to the manager machine. The manager then gets ready to connect to worker machines.

## Step 2: Worker Computation

- Worker machines can now connect to the manager machine

  - Notably, the number of worker machines do not need to be decided at the start of the workflow or determined rigidly in any way. Workers can dynamically enter and exit the workflow as they see fit.

- For each worker machine, the following processes happen:

  1. They receive the input partition parquet, and the download scripts needed for the workflow and the appropriate config file (`download_batch.py`, `single_download.py`, `config.json`)

  2. They start a batch download process using `download_batch.py` (which takes functions from single download)

     - `download_batch.py` initiates a BBR-inspired rate controller called PARC. PARC has a state machine design and it controls the asynchronous downloads of the image files from the host server to maintain optimal throughput against the constraint of server-side rate limiting and server overload.

  3. Once all the items from the partition parquet are downloaded, the entire folder containing all the items is packaged into a tar archive.

  4. A summary JSON file is generated summarizing the statistics of the particular download run on this worker machine.

  5. From here the behavior differs based on where the user wants the data to live:

     - **Manager machine storage:** The worker can send both the tar archive containing the images and the JSON summary back to the manager.

     - **Cloud storage:** The worker performs the CLI API functions needed to push the tar file to the appropriate cloud location and simply sends only the JSON summary back to the manager machine.

- This process continues until all items are downloaded.
