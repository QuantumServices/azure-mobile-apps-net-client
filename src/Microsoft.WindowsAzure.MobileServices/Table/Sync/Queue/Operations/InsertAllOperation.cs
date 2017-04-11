﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Microsoft.WindowsAzure.MobileServices.Sync
{
    internal class InsertAllOperation : MobileServiceTableBulkOperation
    {
        public InsertAllOperation(string tableName, MobileServiceTableKind tableKind, IEnumerable<string> itemIds)
            : base(tableName, tableKind, itemIds)
        {
            this.Operations = itemIds.Select<string, MobileServiceTableOperation>(itemId => new InsertOperation(tableName, tableKind, itemId)).ToList();
        }

        public override MobileServiceTableOperationKind Kind
        {
            get
            {
                return MobileServiceTableOperationKind.Insert;
            }
        }

        public override async Task ExecuteLocalAsync(IMobileServiceLocalStore store, IEnumerable<JObject> items)
        {
            var currentItems = await store.LookupAsync(this.TableName, this.ItemIds);

            if (currentItems != null && currentItems.Any())
            {
                throw new MobileServiceLocalStoreException("An insert operation on one or more of these items is already in the queue.", null);
            }
            await store.UpsertAsync(this.TableName, items, fromServer: false);
        }

        protected override Task<JToken> OnExecuteAsync()
        {
            string unused;
            // for insert operation version should not be sent
            var items = this.Items.Select(item => MobileServiceSerializer.RemoveSystemProperties(item, out unused));
            return this.Table.InsertAsync(items);
        }
    }
}