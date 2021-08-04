using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;
using System.Text;

namespace AzureStorageConsoleDemo.AzureTableDemo
{
    class WorkerModel : TableEntity
    {
        public WorkerModel()
        {
        }

        public WorkerModel(string lastName, string firstName)
        {
            PartitionKey = lastName;
            RowKey = firstName;
        }
        /// <summary>
        /// 职工姓名
        /// </summary>
        public string WorkerName { get; set; }
        /// <summary>
        /// 工号
        /// </summary>
        public string JobNum { get; set; }
        /// <summary>
        /// 部门
        /// </summary>
        public string Department { get; set; }
    }
}
