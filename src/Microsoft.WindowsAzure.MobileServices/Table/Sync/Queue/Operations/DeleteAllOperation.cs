using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

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

        protected override Task<JToken> OnExecuteAsync()
        {
            throw new NotImplementedException();
        }
    }
}