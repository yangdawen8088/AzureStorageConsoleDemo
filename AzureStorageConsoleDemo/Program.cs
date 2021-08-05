using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using AzureStorageConsoleDemo.AzureQueueDemo;
using AzureStorageConsoleDemo.AzureTableDemo;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace AzureStorageConsoleDemo
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //string queueName = "queues" + Guid.NewGuid().ToString().Substring(0, 5);
            string queueName = "queues68f86";
            QueueClient queueClient = await StorageQueuesHelp.CreateQueueClientAsync(queueName);
            //Console.WriteLine("\n正在更新队列中的第三条消息...");

            //// 使用发送消息时保存的回执更新消息
            //await queueClient.UpdateMessageAsync(receipt.MessageId, receipt.PopReceipt, "第三条消息已更新");

            //Console.WriteLine("\n正在接收来自队列的消息...");

            // 从队列中获取消息
            QueueMessage[] messages = await queueClient.ReceiveMessagesAsync(maxMessages: 10);
            Console.WriteLine("\n按 Enter 键“处理”消息并将它们从队列中删除...");
            Console.ReadLine();

            //// 处理和删除队列中的消息
            //foreach (QueueMessage message in messages)
            //{
            //    // “处理”消息
            //    Console.WriteLine($"消息： {message.MessageText}");

            //    // 让服务知道我们已处理完该消息，并且可以安全地将其删除。
            //    await queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt);
            //}
            //Console.WriteLine("\n按 Enter 键删除队列...");
            //Console.ReadLine();

            //// 清理
            //Console.WriteLine($"删除队列： {queueClient.Name}");
            //await queueClient.DeleteAsync();
            Console.ReadLine();
        }
    }
}