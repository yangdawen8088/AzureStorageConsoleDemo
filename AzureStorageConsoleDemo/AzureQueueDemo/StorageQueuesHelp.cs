using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AzureStorageConsoleDemo.AzureQueueDemo
{
    class StorageQueuesHelp
    {
        /// <summary>
        /// Storage 连接字符串
        /// </summary>
        public string StorageConnectionString { get; set; }

        /// <summary>
        /// 初始化程序配置文件
        /// </summary>
        /// <returns>StorageTableHelp</returns>
        public static StorageQueuesHelp LoadAppSettings()
        {
            IConfigurationRoot configRoot = new ConfigurationBuilder().AddJsonFile("Settings.json").Build();
            StorageQueuesHelp appSettings = configRoot.Get<StorageQueuesHelp>();
            return appSettings;
        }

        /// <summary>
        /// 创建一个 Queues
        /// </summary>
        /// <param name="queuesName">Queues 的名称</param>
        /// <returns>QueueClient</returns>
        public static async Task<QueueClient> CreateQueueClientAsync(string queuesName)
        {
            string connectionString = LoadAppSettings().StorageConnectionString;
            QueueClient queueClient = new QueueClient(connectionString, queuesName);
            await queueClient.CreateAsync();
            return queueClient;
        }

        /// <summary>
        /// 向队列中插入一条消息 默认永不过期
        /// </summary>
        /// <param name="theQueue">QueueClient</param>
        /// <param name="newMessage">消息字符串</param>
        /// <returns></returns>
        public static async Task<SendReceipt> InsertMessageAsync(QueueClient theQueue, string newMessage, bool isInvalid = false)
        {
            // 检测队列是否创建，如果没有创建则创建，否则不做操作
            await theQueue.CreateIfNotExistsAsync();
            // 向队列发送一条新的消息
            SendReceipt sendReceipt;
            if (isInvalid)
            {
                // 默认生存周期为 7 天
                sendReceipt = await theQueue.SendMessageAsync(newMessage);
            }
            else
            {
                // 永不过期消息
                sendReceipt = await theQueue.SendMessageAsync(newMessage, default, TimeSpan.FromSeconds(-1), default);
            }
            return sendReceipt;
        }
    }
}
