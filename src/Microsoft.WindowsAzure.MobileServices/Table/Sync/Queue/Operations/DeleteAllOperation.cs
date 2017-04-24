using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using System.Net;

namespace Microsoft.WindowsAzure.MobileServices.Sync
{
    internal class DeleteAllOperation : MobileServiceTableBulkOperation
    {
        public DeleteAllOperation(string tableName, MobileServiceTableKind tableKind, IEnumerable<string> itemIds)
            : base(tableName, tableKind, itemIds)
        {
            this.Operations = itemIds.Select<string, MobileServiceTableOperation>(itemId => new DeleteOperation(tableName, tableKind, itemId)).ToList();
        }

        public override MobileServiceTableOperationKind Kind
        {
            get
            {
                return MobileServiceTableOperationKind.Delete;
            }
        }

        public override bool CanWriteResultToStore
        {
            get
            {
                return false;
            }
        }

        protected override bool SerializeItemToQueue
        {
            get
            {
                return true;
            }
        }

        public override Task ExecuteLocalAsync(IMobileServiceLocalStore store, IEnumerable<JObject> items)
        {
            return store.DeleteAsync(this.TableName, this.ItemIds);
        }

        protected override async Task<JToken> OnExecuteAsync()
        {
            try
            {
                return await this.Table.DeleteAsync(this.Items);
            }
            catch (MobileServiceInvalidOperationException ex)
            {
                // if the item is already deleted then local store is in-sync with the server state
                if (ex.Response.StatusCode == HttpStatusCode.NotFound)
                {
                    return null;
                }
                throw;
            }
        }
    }
}