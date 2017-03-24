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
        /// The state of the operation
        /// </summary>
        MobileServiceTableOperationState State { get; }

        /// <summary>
        /// The table that the operation will be executed against.
        /// </summary>
        IMobileServiceTable Table { get; }

        /// <summary>
        /// The items associated with the bulk operation.
        /// </summary>
        IEnumerable<JObject> Items { get; set; }

        /// <summary>
        /// Executes the operation against remote table.
        /// </summary>
        Task<IEnumerable<JObject>> ExecuteAsync();

        /// <summary>
        /// Abort the parent push operation.
        /// </summary>
        void AbortPush();
    }
}