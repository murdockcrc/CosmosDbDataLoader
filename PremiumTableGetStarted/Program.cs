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
    using System.Globalization;
    using PremiumTableGetStarted.Models;

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
            if (args.Length >= 1 && args[1] == "Standard")
            {
                connectionString = ConfigurationManager.AppSettings["StandardStorageConnectionString"];
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
            Console.WriteLine($"Reading CSV file {filePath}");
            Console.ForegroundColor = ConsoleColor.Gray;

            int numberOfColumns = 20;   //number of columns to take from the CSV schema
            var entitiesToInsert = new List<ITableEntity>();

            IEnumerable<string> flights = File.ReadAllLines(filePath)
                .Skip(1)
                .ToList();

            flights.ToList().ForEach(flight =>
            {
                string[] tokenized = flight.Split(',')
                    .Take(numberOfColumns)
                    .Select(item => item.Replace("\"", ""))
                    .ToArray();
                try
                {
                    var entity = new FlightFactModel(tokenized[14], Guid.NewGuid().ToString())
                    {
                        Year = Convert.ToInt32(tokenized[0]),
                        Quarter = Convert.ToInt32(tokenized[1]),
                        Month = Convert.ToInt32(tokenized[2]),
                        DayOfMonth = Convert.ToInt32(tokenized[3]),
                        DayOfWeek = Convert.ToInt32(tokenized[4]),
                        FlightDate = DateTime.Parse(tokenized[5], CultureInfo.InvariantCulture),
                        UniqueCarrier = tokenized[6],
                        AirlineID = tokenized[7],
                        Carrier = tokenized[8],
                        TailNum = tokenized[9],
                        FlightNum = tokenized[10],
                        OriginAirportID = Convert.ToInt32(tokenized[11]),
                        OriginAirportSeqID = Convert.ToInt32(tokenized[12]),
                        OriginCityMarketID = Convert.ToInt32(tokenized[13]),
                        Origin = tokenized[14],
                        OriginCityName = tokenized[15],
                        OriginState = tokenized[16],
                        OriginStateFips = tokenized[17],
                        OriginStateName = tokenized[18],
                        OriginWac = tokenized[19]
                    };
                    entitiesToInsert.Add(entity);
                }
                catch
                {

                }
            });
            return entitiesToInsert;
        }

        /// <summary>
        /// Returns a List of TableBatchOpertions for row insertion
        /// </summary>
        /// <param name="group">All entities in this group must have the same PartitionKey</param>
        /// <returns></returns>
        private List<TableBatchOperation> GetTableBatchOperations(IGrouping<string, ITableEntity> group)
        {
            var operationsList = new List<TableBatchOperation>();
            var entities = group.ToList();
            var chunkSize = 100;

            while (entities.Any())
            {
                TableBatchOperation batchOperation = new TableBatchOperation();
                var chunk = entities.Take(chunkSize).ToList();
                chunk.ForEach(entity =>
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
        private async Task InsertBatchOperationsAsync(CloudTableClient tableClient, IEnumerable<TableBatchOperation> batches)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("Inserting batches into database");
            Console.ForegroundColor = ConsoleColor.Gray;

            var counter = 0;
            CloudTable table = tableClient.GetTableReference("flights");
            table.CreateIfNotExists();

            foreach (var batch in batches)
            {
                try
                {
                    counter += batch.Count;
                    Stopwatch watch = new Stopwatch();
                    watch.Start();
                    await table.ExecuteBatchAsync(batch);
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
                            Console.ForegroundColor = ConsoleColor.DarkMagenta;
                            Console.WriteLine($"HTTP 429: Throttled");
                            Console.ForegroundColor = ConsoleColor.Gray;

                            System.Threading.Thread.Sleep(1000);
                            await table.ExecuteBatchAsync(batch);
                            break;
                        default:
                            throw storageException;
                    }
                }
                catch (Exception e)
                {
                    throw e;
                }
            }
            Console.WriteLine($"Total inserted rows: {counter}");
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
            var groupedList = new List<IGrouping<string, ITableEntity>>();
            var batchInsertOperations = new ConcurrentBag<TableBatchOperation>();

            foreach (string filePath in filesToImport)
            {
                groupedList = ReadCsvFile(filePath).GroupBy(o => o.PartitionKey).ToList();

                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"Building table batch insert operations for {groupedList.Count} groups");
                Console.ForegroundColor = ConsoleColor.Gray;

                groupedList.AsParallel().ForAll(group =>
                {
                    var batch = GetTableBatchOperations(group);
                    batch.ForEach(x => batchInsertOperations.Add(x));
                });

                await InsertBatchOperationsAsync(tableClient, batchInsertOperations);
                Console.WriteLine("Press Enter to end...");
                Console.ReadLine();
            }
        }
    }
}