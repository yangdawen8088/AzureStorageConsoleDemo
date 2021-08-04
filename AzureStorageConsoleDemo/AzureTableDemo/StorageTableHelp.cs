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
        /// <param name="table">CloudTable</param>
        /// <param name="entity">WorkerModel</param>
        /// <returns></returns>
        public static async Task<WorkerModel> InsertOrMergeEntityAsync(CloudTable table, WorkerModel entity)
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
                WorkerModel insertedCustomer = result.Result as WorkerModel;
                return insertedCustomer;
            }
            catch (StorageException e)
            {
                throw new StorageException(e.Message);
            }
        }

        /// <summary>
        /// 通过 partitionKey 与 rowKey 检索一条数据
        /// </summary>
        /// <param name="table">CloudTable</param>
        /// <param name="partitionKey">partitionKey</param>
        /// <param name="rowKey">rowKey</param>
        /// <returns>WorkerModel</returns>
        public static async Task<WorkerModel> RetrieveEntityUsingPointQueryAsync(CloudTable table, string partitionKey, string rowKey)
        {
            try
            {
                TableOperation retrieveOperation = TableOperation.Retrieve<WorkerModel>(partitionKey, rowKey);
                TableResult result = await table.ExecuteAsync(retrieveOperation);
                if (result.Result is WorkerModel customer)
                {
                    return customer;
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
        /// <param name="table">CloudTable</param>
        /// <param name="deleteEntity">WorkerModel</param>
        /// <returns></returns>
        public static async Task DeleteEntityAsync(CloudTable table, WorkerModel deleteEntity)
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
    }
}
