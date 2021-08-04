using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AzureStorageConsoleDemo.AzureTableDemo
{
    class StorageTableHelp
    {
        /// <summary>
        /// Storage 连接字符串
        /// </summary>
        public string StorageConnectionString { get; set; }

        /// <summary>
        /// 初始化程序配置文件
        /// </summary>
        /// <returns>StorageTableHelp</returns>
        public static StorageTableHelp LoadAppSettings()
        {
            IConfigurationRoot configRoot = new ConfigurationBuilder().AddJsonFile("Settings.json").Build();
            StorageTableHelp appSettings = configRoot.Get<StorageTableHelp>();
            return appSettings;
        }

        /// <summary>
        /// 通过链接字符串创建存储账户，并检测其是否为有效链接
        /// </summary>
        /// <param name="storageConnectionString">Storage 连接字符串</param>
        /// <returns>CloudStorageAccount</returns>
        public static CloudStorageAccount CreateStorageAccountFromConnectionString(string storageConnectionString)
        {
            CloudStorageAccount storageAccount;
            try
            {
                storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            }
            catch (FormatException)
            {
                throw new FormatException("提供的存储帐户信息无效。 请确认 app.config 文件中的 AccountName 和 AccountKey 有效 - 然后重新启动应用程序。");
            }
            catch (ArgumentException)
            {
                throw new ArgumentException("提供的存储帐户信息无效。 请确认 app.config 文件中的 AccountName 和 AccountKey 有效 - 然后重新启动示例。");
            }
            return storageAccount;
        }

        /// <summary>
        /// 异步创建存储表，不存在则创建，存在则不创建
        /// </summary>
        /// <param name="tableName">存储表名称</param>
        /// <returns>CloudTable</returns>
        public static async Task<CloudTable> CreateTableAsync(string tableName)
        {
            string storageConnectionString = LoadAppSettings().StorageConnectionString;
            // 从连接字符串中检索存储帐户信息。
            CloudStorageAccount storageAccount = CreateStorageAccountFromConnectionString(storageConnectionString);
            // 创建用于与表服务交互的表客户端
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient(new TableClientConfiguration());
            // 创建用于与表服务交互的表客户端
            CloudTable table = tableClient.GetTableReference(tableName);
            // 创建表
            await table.CreateIfNotExistsAsync();
            return table;
        }

        /// <summary>
        /// 删除存储表
        /// </summary>
        /// <param name="table">CloudTable</param>
        /// <returns></returns>
        public static async Task DeleteTableAsync(CloudTable table)
        {
            await table.DeleteIfExistsAsync();
        }
        /// <summary>
        /// 插入数据，如果分区与行键值相同则修改该条数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static async Task<T> InsertOrMergeEntityAsync<T>(CloudTable table, T entity) where T : TableEntity
        {
            if (entity == null)
            {
                throw new ArgumentNullException("实体不允许为 null");
            }
            try
            {
                // 创建 InsertOrReplace 表操作
                TableOperation insertOrMergeOperation = TableOperation.InsertOrMerge(entity);
                // 执行操作。
                TableResult result = await table.ExecuteAsync(insertOrMergeOperation);
                return result.Result as T;
            }
            catch (StorageException e)
            {
                throw new StorageException(e.Message);
            }
        }

        /// <summary>
        /// 通过 partitionKey 与 rowKey 检索一条数据
        /// </summary>
        /// <typeparam name="T">实体模型</typeparam>
        /// <param name="table">CloudTable</param>
        /// <param name="partitionKey">partitionKey</param>
        /// <param name="rowKey">rowKey</param>
        /// <returns></returns>
        public static async Task<T> RetrieveEntityUsingPointQueryAsync<T>(CloudTable table, string partitionKey, string rowKey) where T : TableEntity
        {
            try
            {
                TableOperation retrieveOperation = TableOperation.Retrieve<T>(partitionKey, rowKey);
                TableResult result = await table.ExecuteAsync(retrieveOperation);
                if (result.Result is T customer)
                {
                    return result.Result as T;
                }
                return null;
            }
            catch (StorageException e)
            {
                throw new StorageException(e.Message);
            }
        }

        /// <summary>
        /// 删除一条数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">CloudTable</param>
        /// <param name="deleteEntity">数据对象</param>
        /// <returns></returns>
        public static async Task DeleteEntityAsync<T>(CloudTable table, T deleteEntity) where T : TableEntity
        {
            try
            {
                if (deleteEntity == null)
                {
                    throw new ArgumentNullException("实体不存在");
                }
                TableOperation deleteOperation = TableOperation.Delete(deleteEntity);
                TableResult result = await table.ExecuteAsync(deleteOperation);
            }
            catch (StorageException e)
            {
                throw new StorageException(e.Message);
            }
        }

        /// <summary>
        /// 列出 CloudTableClient 下所有的数据表
        /// </summary>
        /// <param name="tableClient">CloudTableClient</param>
        /// <returns></returns>
        public static IEnumerable<CloudTable> ListTables(CloudTableClient tableClient)
        {
            try
            {
                return tableClient.ListTables();
            }
            catch (StorageException e)
            {
                throw new StorageException(e.Message);
            }
        }

        /// <summary>
        /// 列出 CloudTableClient 下所有名称以 prefix 开头的数据表
        /// </summary>
        /// <param name="tableClient">CloudTableClient</param>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static IEnumerable<CloudTable> ListTables(CloudTableClient tableClient, string prefix)
        {
            try
            {
                TableContinuationToken continuationToken = null;
                TableResultSegment resultSegment = tableClient.ListTablesSegmented(prefix, continuationToken);
                return resultSegment.Results;
            }
            catch (StorageException e)
            {
                throw new StorageException(e.Message);
            }
        }

        /// <summary>
        /// 批量插入数据
        /// </summary>
        /// <typeparam name="T">实体模型</typeparam>
        /// <param name="table">CloudTable</param>
        /// <param name="entityList">数据集</param>
        /// <returns></returns>
        public static async Task<TableBatchResult> BatchInsertEntitiesAsync<T>(CloudTable table, List<T> entityList) where T : TableEntity
        {
            try
            {
                // 创建批处理操作。
                TableBatchOperation batchOperation = new TableBatchOperation();
                // 将需要插入的数据加入到批处理操作中
                entityList.ForEach(entity =>
                {
                    batchOperation.Insert(entity);
                });
                // 执行批处理操作。
                TableBatchResult results = await table.ExecuteBatchAsync(batchOperation);
                return results;
            }
            catch (StorageException e)
            {
                throw new StorageException(e.Message);
            }
        }

        /// <summary>
        /// 查询一个分区中的所有数据
        /// </summary>
        /// <param name="table">CloudTable</param>
        /// <param name="partitionKey">分区名称</param>
        /// <returns></returns>
        public static async Task<List<WorkerModel>> PartitionScanAsync(CloudTable table, string partitionKey)
        {
            TableQuery<WorkerModel> partitionScanQuery =
                    new TableQuery<WorkerModel>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));
            TableContinuationToken token = null;
            TableQuerySegment<WorkerModel> segment = await table.ExecuteQuerySegmentedAsync(partitionScanQuery, token);
            List<WorkerModel> entitys = new List<WorkerModel>();
            entitys.AddRange(segment);
            return entitys;
        }
    }
}
