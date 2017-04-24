using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.MobileServices.Sync
{
    internal abstract class MobileServiceTableBulkOperation : IMobileServiceTableBulkOperation
    {
        // --- Persisted properties -- //
        public abstract MobileServiceTableOperationKind Kind { get; }

        public MobileServiceTableKind TableKind { get; private set; }
        public string TableName { get; private set; }

        public IEnumerable<string> ItemIds
        {
            get
            {
                return Operations.Select(op => op.ItemId);
            }
        }

        public IEnumerable<JObject> Items
        {
            get
            {
                return Operations.Select(op => op.Item);
            }
        }

        public ICollection<MobileServiceTableOperation> Operations { get; internal set; }

        public long ItemCount
        {
            get
            {
                return Operations.Where(op => !op.IsCancelled).LongCount();
            }
        }

        // --- Non persisted properties -- //
        IMobileServiceTable IMobileServiceTableBulkOperation.Table
        {
            get { return this.Table; }
        }

        public MobileServiceTable Table { get; set; }

        public virtual bool CanWriteResultToStore
        {
            get { return true; }
        }

        protected virtual bool SerializeItemToQueue
        {
            get { return false; }
        }

        protected MobileServiceTableBulkOperation(string tableName, MobileServiceTableKind tableKind, IEnumerable<string> itemIds)
        {
            this.TableKind = tableKind;
            this.TableName = tableName;
        }

        public void AbortPush()
        {
            throw new MobileServicePushAbortException();
        }

        public async Task<IEnumerable<JObject>> ExecuteAsync()
        {
            if (this.Operations.Any(op => op.Item == null))
            {
                throw new MobileServiceInvalidOperationException("Operation must have an items associated with it.", request: null, response: null);
            }

            JToken response = await OnExecuteAsync();
            var result = response as JArray;
            if (response != null && result == null)
            {
                throw new MobileServiceInvalidOperationException("Mobile Service table operation returned an unexpected response.", request: null, response: null);
            }

            // delete returns an empty result
            result = this.Kind == MobileServiceTableOperationKind.Delete ? new JArray() : result;

            return result.ToObject<IEnumerable<JObject>>();
        }

        protected abstract Task<JToken> OnExecuteAsync();

        /// <summary>
        /// Execute the operation on sync store
        /// </summary>
        /// <param name="store">Sync store</param>
        /// <param name="item">The item to use for store operation</param>
        public abstract Task ExecuteLocalAsync(IMobileServiceLocalStore store, IEnumerable<JObject> items);

        internal void SetOperationSequence(long startSquence)
        {
            foreach (var op in this.Operations)
            {
                op.Sequence = startSquence++;
            }
        }

        internal IEnumerable<JObject> Serialize()
        {
            return this.Operations
                .Where(op => !op.IsCancelled)
                .Select(op => new JObject()
                    {
                        { MobileServiceSystemColumns.Id, op.Id },
                        { "kind", (int)this.Kind },
                        { "state", (int)op.State },
                        { "tableName", this.TableName },
                        { "tableKind", (int)this.TableKind },
                        { "itemId", op.ItemId },
                        { "item", op.Item != null && this.SerializeItemToQueue ? op.Item.ToString(Formatting.None) : null},
                        { "sequence", op.Sequence },
                        { "version",  op.Version }
                    }).ToList();
        }

        internal static MobileServiceTableBulkOperation Deserialize(IEnumerable<JObject> objects)
        {
            if (objects == null)
            {
                return null;
            }

            // Use first object to determine the operation kind, tableName and tableKind
            JObject first = objects.FirstOrDefault();
            if (first == null)
            {
                return null;
            }
            var kind = (MobileServiceTableOperationKind)first.Value<int>("kind");
            string tableName = first.Value<string>("tableName");
            var tableKind = (MobileServiceTableKind)first.Value<int?>("tableKind").GetValueOrDefault();

            // make sure all the objects have the same operation kind, table name and table kind
            if (objects.Any(obj => obj.Value<int>("kind") != (int)kind)
                || objects.Any(obj => obj.Value<string>("tableName") != tableName)
                || objects.Any(obj => (MobileServiceTableKind)obj.Value<int?>("tableKind").GetValueOrDefault() != tableKind))
            {
                throw new ArgumentException("All operations should be of the same kind, for the same table and table kind", "objects");
            }

            MobileServiceTableBulkOperation bulkOperation = null;
            switch (kind)
            {
                case MobileServiceTableOperationKind.Insert:
                    bulkOperation = new InsertAllOperation(tableName, tableKind, new List<string>()); break;
                case MobileServiceTableOperationKind.Update:
                    bulkOperation = new UpdateAllOperation(tableName, tableKind, new List<string>()); break;
                case MobileServiceTableOperationKind.Delete:
                    bulkOperation = new DeleteAllOperation(tableName, tableKind, new List<string>()); break;
            }

            if (bulkOperation != null)
            {
                bulkOperation.Operations = objects.Select(op => MobileServiceTableOperation.Deserialize(op)).ToList();
            }

            return bulkOperation;
        }
    }
}