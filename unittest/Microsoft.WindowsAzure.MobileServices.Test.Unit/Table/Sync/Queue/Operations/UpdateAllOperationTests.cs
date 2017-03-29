using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.MobileServices.Sync;
using Moq;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.MobileServices.Test.Unit.Table.Sync.Queue.Operations
{
    [TestClass]
    public class UpdateAllOperationTests
    {
        private UpdateAllOperation operation;
        private long bulkInsertCount = 10000;

        [TestInitialize]
        public void Initialize()
        {
            IList<string> ids = new List<string>();
            for (int i = 0; i < bulkInsertCount; i++)
            {
                ids.Add("abc" + i);
            }
            this.operation = new UpdateAllOperation("test", MobileServiceTableKind.Table, ids);
        }

        [TestMethod]
        public async Task ExecuteLocalAsync_UpsertsItemsOnStore()
        {
            var store = new Mock<IMobileServiceLocalStore>();
            IList<JObject> items = new List<JObject>();
            for (long i = 0; i < bulkInsertCount; i++)
            {
                var item = JObject.Parse($"{{\"id\":\"abc{i}\",\"Text\":\"Example\"}}");
                items.Add(item);
            }
            await this.operation.ExecuteLocalAsync(store.Object, items);
            store.Verify(s => s.UpsertAsync("test", It.IsIn<IEnumerable<JObject>>(items), false), Times.Once());
        }
    }
}