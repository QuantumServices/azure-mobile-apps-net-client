using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Microsoft.WindowsAzure.MobileServices.Sync
{
    internal class UpdateAllOperation : MobileServiceTableBulkOperation
    {
        public UpdateAllOperation(string tableName, MobileServiceTableKind tableKind, IEnumerable<string> itemIds)
            : base(tableName, tableKind, itemIds)
        {
            this.Operations = itemIds.Select<string, MobileServiceTableOperation>(itemId => new UpdateOperation(tableName, tableKind, itemId)).ToList();
        }

        public override MobileServiceTableOperationKind Kind
        {
            get
            {
                return MobileServiceTableOperationKind.Update;
            }
        }

        public override Task ExecuteLocalAsync(IMobileServiceLocalStore store, IEnumerable<JObject> items)
        {
            return store.UpsertAsync(this.TableName, items, fromServer: false);
        }

        protected override Task<JToken> OnExecuteAsync()
        {
            return this.Table.UpdateAsync(this.Items);
        }
    }
}