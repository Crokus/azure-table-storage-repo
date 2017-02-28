using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Wolnik.Azure.TableStorage.Repository
{
    public class AzureTableStorage : ITableStorage
    {
        private readonly CloudTableClient _client;
        private bool _tableExits;
        private CloudTable _table;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureTableStorage" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public AzureTableStorage(string connectionString)
        {
            CloudStorageAccount account = CloudStorageAccount.Parse(connectionString);
            _client = account.CreateCloudTableClient();
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
            await EnsureTable(tableName);

            TableOperation retrieveOperation = TableOperation.Retrieve<T>(partitionKey, rowKey);

            TableResult result = await _table.ExecuteAsync(retrieveOperation);

            return result.Result as T;
        }

        public async Task<IEnumerable<T>> GetAllAsync<T>(string tableName) where T : class, ITableEntity, new()
        {
            await EnsureTable(tableName);

            TableContinuationToken token = null;
            var entities = new List<T>();
            do
            {
                var queryResult = await _table.ExecuteQuerySegmentedAsync(new TableQuery<T>(), token);
                entities.AddRange(queryResult.Results);
                token = queryResult.ContinuationToken;
            } while (token != null);

            return entities;
        }

        /// <summary>
        /// Gets entities by query
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public async Task<IEnumerable<T>> QueryAsync<T>(string tableName, TableQuery<T> query) where T : class, ITableEntity, new()
        {
            await EnsureTable(tableName);

            TableContinuationToken token = null;
            var entities = new List<T>();
            do
            {
                var queryResult = await _table.ExecuteQuerySegmentedAsync(query, token);
                entities.AddRange(queryResult.Results);
                token = queryResult.ContinuationToken;
            } while (token != null);

            return entities;
        }

        /// <summary>
        /// Adds the or update entity.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public async Task<object> AddOrUpdateAsync(string tableName, ITableEntity entity)
        {
            await EnsureTable(tableName);

            TableOperation insertOrReplaceOperation = TableOperation.InsertOrReplace(entity);

            TableResult result = await _table.ExecuteAsync(insertOrReplaceOperation);

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
            await EnsureTable(tableName);

            TableOperation deleteOperation = TableOperation.Delete(entity);

            TableResult result = await _table.ExecuteAsync(deleteOperation);

            return result.Result;
        }

        /// <summary>
        /// Ensures existance of the table.
        /// </summary>
        private async Task EnsureTable(string tableName)
        {
            if (!_tableExits)
            {
                _table = _client.GetTableReference(tableName);
                await _table.CreateIfNotExistsAsync();
                _tableExits = true;
            }
        }
    }
}
