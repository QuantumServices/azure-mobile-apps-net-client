using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
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

        public IEnumerable<MobileServiceTableOperation> Operations { get; internal set; }
        public long StartSequence { get; set; }

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

            IEnumerable<JToken> response = await OnExecuteAsync();
            var result = response as IEnumerable<JObject>;
            if (response != null && result == null)
            {
                throw new MobileServiceInvalidOperationException("Mobile Service table operation returned an unexpected response.", request: null, response: null);
            }

            return result;
        }

        protected abstract Task<IEnumerable<JToken>> OnExecuteAsync();

        /// <summary>
        /// Execute the operation on sync store
        /// </summary>
        /// <param name="store">Sync store</param>
        /// <param name="item">The item to use for store operation</param>
        public abstract Task ExecuteLocalAsync(IMobileServiceLocalStore store, IEnumerable<JObject> items);

        internal IEnumerable<JObject> Serialize()
        {
            //Use the sequence as the start of the sequence
            long sequence = this.StartSequence;
            return Operations
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
                        { "sequence", op.Sequence = sequence++ },
                        { "version",  op.Version }
                    }).ToList();
        }

        internal static MobileServiceTableBulkOperation Deserialize(IEnumerable<JObject> objects)
        {
            //    if (objects == null)
            //    {
            //        return null;
            //    }

            //    // Use first object to determine the
            //    JObject obj = objects.FirstOrDefault();
            //    if (obj == null)
            //    {
            //        return null;
            //    }
            //    var kind = (MobileServiceTableOperationKind)obj.Value<int>("kind");
            //    string tableName = obj.Value<string>("tableName");
            //    var tableKind = (MobileServiceTableKind)obj.Value<int?>("tableKind").GetValueOrDefault();
            //    string itemId = obj.Value<string>("itemId");

            //    MobileServiceTableOperation operation = null;
            //    switch (kind)
            //    {
            //        case MobileServiceTableOperationKind.Insert:
            //            operation = new InsertOperation(tableName, tableKind, itemId); break;
            //        case MobileServiceTableOperationKind.Update:
            //            operation = new UpdateOperation(tableName, tableKind, itemId); break;
            //        case MobileServiceTableOperationKind.Delete:
            //            operation = new DeleteOperation(tableName, tableKind, itemId); break;
            //    }

            //    if (operation != null)
            //    {
            //        operation.Id = obj.Value<string>(MobileServiceSystemColumns.Id);
            //        operation.Sequence = obj.Value<long?>("sequence").GetValueOrDefault();
            //        operation.Version = obj.Value<long?>("version").GetValueOrDefault();
            //        string itemJson = obj.Value<string>("item");
            //        operation.Item = !String.IsNullOrEmpty(itemJson) ? JObject.Parse(itemJson) : null;
            //        operation.State = (MobileServiceTableOperationState)obj.Value<int?>("state").GetValueOrDefault();
            //    }

            //    return operation;
            throw new NotImplementedException();
        }
    }
}