using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.MobileServices.Sync
{
    public interface IMobileServiceTableBulkOperation
    {
        /// <summary>
        /// The kind of operation
        /// </summary>
        MobileServiceTableOperationKind Kind { get; }

        /// <summary>
        /// The table that the operation will be executed against.
        /// </summary>
        IMobileServiceTable Table { get; }

        /// <summary>
        /// Items associated with this operation
        /// </summary>
        IEnumerable<JObject> Items { get; }

        /// <summary>
        /// Executes the operation against remote table.
        /// </summary>
        Task<IEnumerable<JObject>> ExecuteAsync();

        /// <summary>
        /// Abort the parent push operation.
        /// </summary>
        void AbortPush();

        /// <summary>
        /// Updates the items for this operation, does not update them in the table.
        /// </summary>
        /// <param name="items"></param>
        void UpdateItems(IEnumerable<JObject> items);

        /// <summary>
        /// Excludes these items from from local and remote operation
        /// </summary>
        /// <param name="items"></param>
        void ExcludeItems(IEnumerable<JObject> items);
    }
}