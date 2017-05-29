namespace TableSBS
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using System.Linq;
    using System.IO;
    using System.Threading.Tasks;
    using System.Collections.Concurrent;
    using Microsoft.VisualBasic;
    using Microsoft.VisualBasic.FileIO;

    /// <summary>
    /// This sample program shows how to use the Azure storage SDK to work with premium tables (created using the Azure Cosmos DB service)
    /// </summary>
    public class Program
    {
        /// <summary>
        /// Run common Table CRUD and query operations using the Azure Cosmos DB endpoints ("premium tables")
        /// </summary>
        /// <param name="args">Command line arguments</param>
        public static void Main(string[] args)
        {
            string folderPath = null;
            string connectionString = ConfigurationManager.AppSettings["PremiumStorageConnectionString"];

            if (args.Length >= 0)
            {
               folderPath = args[0];
            }

            if (folderPath == null)
            {
                throw new Exception("Need to provide a folder path");
            }

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            Program p = new Program();

            Task.Run(async () =>
            {
                await p.Run(tableClient, folderPath);
            }).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Retrieves all files in the supplied path. The program expects this folder to contains only CSV files, all with the same schema
        /// </summary>
        /// <param name="folderPath">Absolute path to the folder where CSV files are stored</param>
        /// <returns></returns>
        private string[] GetFiles(string folderPath)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("Retrieving files to import");
            Console.ForegroundColor = ConsoleColor.Gray;
            return Directory.GetFiles(folderPath);
        }

        /// <summary>
        /// Reads the CSV and returns a List with all rows converted into an instance of ITableEntity
        /// </summary>
        /// <param name="filePath">Absolute path of the file to read</param>
        /// <returns></returns>
        private List<ITableEntity> ReadCsvFile(string filePath)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("Retrieving entities to insert");
            Console.ForegroundColor = ConsoleColor.Gray;

            var entitiesToInsert = new List<ITableEntity>();
            using (TextFieldParser parser = new TextFieldParser(filePath))
            {
                int numberOfColumns = 50;   //number of columns to take from the CSV schema
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(",");

                var headers = parser.ReadFields().Take(numberOfColumns);

                while (!parser.EndOfData)
                {
                    try
                    {
                        // Uncomment this if you want to add a limit to the amount of rows to read per CSV file
                        if (parser.LineNumber == 1000)
                        {
                            return entitiesToInsert;
                        }

                        Console.WriteLine($"Processing line {parser.LineNumber}");

                        var fields = parser.ReadFields().Take(numberOfColumns);
                        var guid = Guid.NewGuid().ToString();
                        var entity = new DynamicTableEntity(fields.ElementAt(14), guid);    // Uses airport IATA code as partition key

                        var properties = new Dictionary<string, EntityProperty>();
                        for (int i = 0; i < numberOfColumns; i++)
                        {
                            properties.Add(headers.ElementAt(i),
                                new EntityProperty(fields.ElementAt(i)));
                        }
                        entity.Properties = properties;
                        entitiesToInsert.Add(entity);
                    }
                    catch
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"ERROR: failed to parse line {parser.LineNumber}");
                        Console.ForegroundColor = ConsoleColor.Gray;
                    }
                }
            }
            return entitiesToInsert;
        }

        /// <summary>
        /// Returns a List of TableBatchOpertions for row insertion
        /// </summary>
        /// <param name="group">All entities in this group must have the same PartitionKey</param>
        /// <returns></returns>
        private List<TableBatchOperation> GetTableBatchOperations(IGrouping<string, ITableEntity> group)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"Inserting entities for batch {group.Key}");
            Console.ForegroundColor = ConsoleColor.Gray;

            var operationsList = new List<TableBatchOperation>();
            var entities = group.ToList();
            var chunkSize = 100;       

            while(entities.Any())
            {
                TableBatchOperation batchOperation = new TableBatchOperation();
                var chunk = entities.Take(chunkSize).ToList();
                chunk.AsParallel().ForAll(entity =>
                {
                    try
                    {
                        batchOperation.Insert(entity);
                    }
                    catch { }
                });
                operationsList.Add(batchOperation);
                entities = entities.Skip(chunkSize).ToList();
            }
            return operationsList;
        }

        /// <summary>
        /// Executes the supplied batch operations against the table
        /// </summary>
        /// <param name="tableClient"></param>
        /// <param name="batches"></param>
        /// <returns></returns>
        private async Task InsertBatchOperationsAsync(CloudTableClient tableClient, IList<TableBatchOperation> batches)
        {
            var counter = 0;

            CloudTable table = tableClient.GetTableReference("flights");
            table.CreateIfNotExists();

            foreach(var batch in batches)
            {
                try
                {
                    counter += batch.Count;
                    Console.WriteLine($"Batches counter: {counter}");

                    Stopwatch watch = new Stopwatch();
                    watch.Start();
                    var task = table.ExecuteBatchAsync(batch);
                    await task;
                    watch.Stop();
                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-ddThh:mm:ssK")}, {watch.ElapsedMilliseconds.ToString()}, {batch.Count}");
                    watch.Reset();
                }
                catch (StorageException storageException)
                {
                    switch (storageException.RequestInformation.HttpStatusCode)
                    {
                        case 409:
                            // Entity already exists, ignore
                            break;
                        case 429:
                            System.Threading.Thread.Sleep(1000);
                            await table.ExecuteBatchAsync(batch);
                            break;
                        default:
                            throw storageException;
                    }
                }
                catch(Exception e)
                {
                    throw e;
                }
            }
        }

        /// <summary>
        /// Main method that gets the insert operation going
        /// </summary>
        /// <param name="tableClient"></param>
        /// <param name="folderPath"></param>
        /// <returns></returns>
        public async Task Run(CloudTableClient tableClient, string folderPath)
        {
            var filesToImport = GetFiles(folderPath);
            List<IGrouping<string, ITableEntity>> groupedList;
            List<TableBatchOperation> batchInsertOperations;

            foreach (string filePath in filesToImport)
            {
                batchInsertOperations = new List<TableBatchOperation>();
                groupedList = new List<IGrouping<string, ITableEntity>>();

                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"Uploaded from {filePath}");
                Console.ForegroundColor = ConsoleColor.Gray;

                groupedList = ReadCsvFile(filePath).GroupBy(o => o.PartitionKey).ToList();
                groupedList.AsParallel().ForAll(group =>
                {
                    batchInsertOperations.AddRange(GetTableBatchOperations(group));
                });
           
                await InsertBatchOperationsAsync(tableClient, batchInsertOperations);
            }

            Console.WriteLine("Press any key to end...");
            Console.ReadLine();
        }
    }
}