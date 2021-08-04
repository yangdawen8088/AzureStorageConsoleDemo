using AzureStorageConsoleDemo.AzureTableDemo;
using Microsoft.Azure.Cosmos.Table;
using System;
using System.Data;
using System.Threading.Tasks;

namespace AzureStorageConsoleDemo
{
    class Program
    {
        static async Task Main(string[] args)
        {

            Console.WriteLine("Azure Cosmos DB 表 - 基本示例\n");
            Console.WriteLine();

            string tableName = "demo" + Guid.NewGuid().ToString().Substring(0, 5);

            // 创建或引用现有表
            CloudTable table =await StorageTableHelp.CreateTableAsync(tableName);

            try
            {
                // 演示基本的 CRUD 功能
                await BasicDataOperationsAsync(table);
            }
            finally
            {
                // 删除表
                await StorageTableHelp.DeleteTableAsync(table);
            }
        }
        private static async Task BasicDataOperationsAsync(CloudTable table)
        {
            // 创建客户实体的实例。 有关实体的描述，请参阅 Model\CustomerEntity.cs。
            string keystr = Guid.NewGuid().ToString();
            WorkerModel customer = new WorkerModel("Harp", keystr)
            {
                WorkerName = "张三",
                JobNum = "20210728",
                Department = "外交部"
            };

            // 演示如何插入实体
            Console.WriteLine("插入一个实体。");
            customer = await StorageTableHelp.InsertOrMergeEntityAsync(table, customer);

            // 演示如何通过更改部门来更新实体
            Console.WriteLine("使用 InsertOrMerge Upsert 操作更新现有实体。");
            customer.Department = "策划部";
            await StorageTableHelp.InsertOrMergeEntityAsync(table, customer);
            Console.WriteLine();

            // 演示如何使用点查询读取更新的实体
            Console.WriteLine("读取更新后的实体。");
            customer = await StorageTableHelp.RetrieveEntityUsingPointQueryAsync(table, "Harp", keystr);
            Console.WriteLine();

            // 演示如何删除实体
            Console.WriteLine("删除实体。");
            Console.WriteLine("\t{0}\t{1}\t{2}\t{3}\t{4}", customer.PartitionKey, customer.RowKey, customer.WorkerName, customer.JobNum, customer.Department);
            await StorageTableHelp.DeleteEntityAsync(table, customer);
            Console.WriteLine();
        }
    }
}