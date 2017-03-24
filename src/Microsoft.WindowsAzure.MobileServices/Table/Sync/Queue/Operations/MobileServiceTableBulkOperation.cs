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
        public IEnumerable<string> ItemIds { get; private set; }
        public IEnumerable<JObject> Items { get; set; }

        public MobileServiceTableOperationState State { get; internal set; }
        public long Sequence { get; set; }
        public long Version { get; set; }

        public long ItemCount
        {
            get
            {
                return Items.LongCount();
            }
        }

        // --- Non persisted properties -- //
        IMobileServiceTable IMobileServiceTableBulkOperation.Table
        {
            get { return this.Table; }
        }

        public MobileServiceTable Table { get; set; }

        public bool IsCancelled { get; private set; }
        public bool IsUpdated { get; private set; }

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
            this.State = MobileServiceTableOperationState.Pending;
            this.TableKind = tableKind;
            this.TableName = tableName;
            this.ItemIds = itemIds;
            this.Version = 1;
        }

        public void AbortPush()
        {
            throw new MobileServicePushAbortException();
        }

        public async Task<IEnumerable<JObject>> ExecuteAsync()
        {
            throw new NotImplementedException();
            //if (this.IsCancelled)
            //{
            //    return null;
            //}

            //if (this.Item == null)
            //{
            //    throw new MobileServiceInvalidOperationException("Operation must have an item associated with it.", request: null, response: null);
            //}

            //IEnumerable<JToken> response = await OnExecuteAsync();
            //var result = response as IEnumerable<JObject>;
            //if (response != null && result == null)
            //{
            //    throw new MobileServiceInvalidOperationException("Mobile Service table operation returned an unexpected response.", request: null, response: null);
            //}

            //return result;
        }

        protected abstract Task<IEnumerable<JToken>> OnExecuteAsync();

        internal void Cancel()
        {
            this.IsCancelled = true;
        }

        internal void Update()
        {
            this.Version++;
            this.IsUpdated = true;
        }

        /// <summary>
        /// Execute the operation on sync store
        /// </summary>
        /// <param name="store">Sync store</param>
        /// <param name="item">The item to use for store operation</param>
        public abstract Task ExecuteLocalAsync(IMobileServiceLocalStore store, IEnumerable<JObject> items);

        /// <summary>
        /// Validates that the operation can collapse with the late operation
        /// </summary>
        /// <exception cref="InvalidOperationException">This method throws when the operation cannot collapse with new operation.</exception>
        public abstract void Validate(MobileServiceTableOperation newOperation);

        /// <summary>
        /// Collapse this operation with the late operation by cancellation of either operation.
        /// </summary>
        public abstract void Collapse(MobileServiceTableOperation newOperation);

        /// <summary>
        /// Defines the the table for storing operations
        /// </summary>
        /// <param name="store">An instance of <see cref="IMobileServiceLocalStore"/></param>
        internal static void DefineTable(MobileServiceLocalStore store)
        {
            store.DefineTable(MobileServiceLocalSystemTables.OperationQueue, new JObject()
            {
                { MobileServiceSystemColumns.Id, String.Empty },
                { "kind", 0 },
                { "state", 0 },
                { "tableName", String.Empty },
                { "tableKind", 0 },
                { "itemId", String.Empty },
                { "item", String.Empty },
                { MobileServiceSystemColumns.CreatedAt, DateTime.Now },
                { "sequence", 0 },
                { "version", 0 }
            });
        }

        internal IEnumerable<JObject> Serialize()
        {
            //Use the sequence as the start of the sequence
            long sequence = this.Sequence - 1;
            return ItemIds.Select(itemId => new JObject()
            {
                { MobileServiceSystemColumns.Id, Guid.NewGuid().ToString("N") },
                { "kind", (int)this.Kind },
                { "state", (int)this.State },
                { "tableName", this.TableName },
                { "tableKind", (int)this.TableKind },
                { "itemId", itemId },
                { "item",
                    Items != null && this.SerializeItemToQueue ?
                        Items.Single(item => item.Value<string>(MobileServiceSystemColumns.Id) == itemId).ToString(Formatting.None)
                        : null
                },
                { "sequence", sequence++ },
                { "version",  this.Version }
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