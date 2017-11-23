using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Wolnik.Azure.TableStorage.Repository
{
    public class AzureTableStorage : ITableStorage
    {
        private readonly CloudTableClient _client;
        private IDictionary<string, CloudTable> _tables;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureTableStorage" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public AzureTableStorage(string connectionString)
        {
            CloudStorageAccount account = CloudStorageAccount.Parse(connectionString);
            _client = account.CreateCloudTableClient();

            _tables = new Dictionary<string, CloudTable>();
        }

        /// <summary>
        /// Gets the entity.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableName"></param>
        /// <param name="partitionKey">The partition key.</param>
        /// <param name="rowKey">The row key.</param>
        /// <returns></returns>
        public async Task<T> GetAsync<T>(string tableName, string partitionKey, string rowKey) where T : class, ITableEntity
        {
            var table = await EnsureTable(tableName);

            TableOperation retrieveOperation = TableOperation.Retrieve<T>(partitionKey, rowKey);

            TableResult result = await table.ExecuteAsync(retrieveOperation);

            return result.Result as T;
        }

        public async Task<IEnumerable<T>> GetAllAsync<T>(string tableName) where T : class, ITableEntity, new()
        {
            var table = await EnsureTable(tableName);

            TableContinuationToken token = null;
            var entities = new List<T>();
            do
            {
                var queryResult = await table.ExecuteQuerySegmentedAsync(new TableQuery<T>(), token);
                entities.AddRange(queryResult.Results);
                token = queryResult.ContinuationToken;
            } while (token != null);

            return entities;
        }

        /// <summary>
        /// Gets entities by query. 
        /// Supports TakeCount parameter.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public async Task<IEnumerable<T>> QueryAsync<T>(string tableName, TableQuery<T> query) where T : class, ITableEntity, new()
        {
            var table = await EnsureTable(tableName);

            bool shouldConsiderTakeCount = query.TakeCount.HasValue;

            return shouldConsiderTakeCount ? await QueryAsyncWithTakeCount(table, query) : await QueryAsync(table, query);
        }

        /// <summary>
        /// Adds the or update entity.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public async Task<object> AddOrUpdateAsync(string tableName, ITableEntity entity)
        {
            var table = await EnsureTable(tableName);

            TableOperation insertOrReplaceOperation = TableOperation.InsertOrReplace(entity);

            TableResult result = await table.ExecuteAsync(insertOrReplaceOperation);

            return result.Result;
        }

        /// <summary>
        /// Deletes the entity.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public async Task<object> DeleteAsync(string tableName, ITableEntity entity)
        {
            var table = await EnsureTable(tableName);

            TableOperation deleteOperation = TableOperation.Delete(entity);

            TableResult result = await table.ExecuteAsync(deleteOperation);

            return result.Result;
        }

        /// <summary>
        /// Add the entity.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public async Task<object> AddAsync(string tableName, ITableEntity entity)
        {
            var table = await EnsureTable(tableName);

            TableOperation insertOperation = TableOperation.Insert(entity);

            TableResult result = await table.ExecuteAsync(insertOperation);

            return result.Result;
        }

        /// <summary>
        /// Insert a batch of entities. Support adding more than 100 entities.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="entities">Collection of entities.</param>
        /// <param name="options">Batch operation options</param>
        /// <returns></returns>
        public async Task<IEnumerable<T>> AddBatchAsync<T>(string tableName, IEnumerable<ITableEntity> entities, BatchOperationOptions options = null) where T : class, ITableEntity, new()
        {
            var table = await EnsureTable(tableName);

            options = options ?? new BatchOperationOptions();

            var tasks = new List<Task<IList<TableResult>>>();

            const int addBatchOperationLimit = 100;
            var entitiesOffset = 0;
            while (entitiesOffset < entities?.Count())
            {
                var entitiesToAdd = entities.Skip(entitiesOffset).Take(addBatchOperationLimit).ToList();
                entitiesOffset += entitiesToAdd.Count;

                Action<TableBatchOperation, ITableEntity> batchInsertOperation = null;
                switch (options.BatchInsertMethod)
                {
                    case BatchInsertMethod.Insert:
                        batchInsertOperation = (bo, entity) => bo.Insert(entity);
                        break;
                    case BatchInsertMethod.InsertOrReplace:
                        batchInsertOperation = (bo, entity) => bo.InsertOrReplace(entity);
                        break;
                    case BatchInsertMethod.InsertOrMerge:
                        batchInsertOperation = (bo, entity) => bo.InsertOrMerge(entity);
                        break;
                }
                TableBatchOperation batchOperation = new TableBatchOperation();
                foreach (var entity in entitiesToAdd)
                {
                    batchInsertOperation(batchOperation, entity);
                }
                tasks.Add(table.ExecuteBatchAsync(batchOperation));
            }

            var results = await Task.WhenAll(tasks);

            return results.SelectMany(tableResults => tableResults, (tr, r) => r.Result as T);
        }

        /// <summary>
        /// Updates the entity.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public async Task<object> UpdateAsync(string tableName, ITableEntity entity)
        {
            var table = await EnsureTable(tableName);

            TableOperation replaceOperation = TableOperation.Replace(entity);

            TableResult result = await table.ExecuteAsync(replaceOperation);

            return result.Result;
        }

        /// <summary>
        /// Ensures existance of the table.
        /// </summary>
        private async Task<CloudTable> EnsureTable(string tableName)
        {
            if (!_tables.ContainsKey(tableName))
            {
                var table = _client.GetTableReference(tableName);
                await table.CreateIfNotExistsAsync();
                _tables[tableName] = table;
            }

            return _tables[tableName];
        }

        /// <summary>
        /// Gets entities by query
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        private async Task<IEnumerable<T>> QueryAsync<T>(CloudTable table, TableQuery<T> query)
            where T : class, ITableEntity, new()
        {
            var entities = new List<T>();

            TableContinuationToken token = null;
            do
            {
                var queryResult = await table.ExecuteQuerySegmentedAsync(query, token);
                entities.AddRange(queryResult.Results);
                token = queryResult.ContinuationToken;
            } while (token != null);

            return entities;
        }

        /// <summary>
        /// Get entities by query with TakeCount parameter
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        private async Task<IEnumerable<T>> QueryAsyncWithTakeCount<T>(CloudTable table, TableQuery<T> query)
            where T : class, ITableEntity, new()
        {
            var entities = new List<T>();

            const int maxEntitiesPerQueryLimit = 1000;
            var totalTakeCount = query.TakeCount;
            var remainingRecordsToTake = query.TakeCount;

            TableContinuationToken token = null;
            do
            {
                query.TakeCount = remainingRecordsToTake >= maxEntitiesPerQueryLimit ? maxEntitiesPerQueryLimit : remainingRecordsToTake;
                remainingRecordsToTake -= query.TakeCount;

                var queryResult = await table.ExecuteQuerySegmentedAsync(query, token);
                entities.AddRange(queryResult.Results);
                token = queryResult.ContinuationToken;
            } while (entities.Count < totalTakeCount && token != null);

            return entities;
        }
    }
}