using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    // ---------------------------------------------------------------------------
    // Tests: Phase 1-1 LEFT JOIN (GroupJoin + SelectMany + DefaultIfEmpty)
    // ---------------------------------------------------------------------------
    public class QueryableLeftJoinTest : IDisposable
    {
        readonly string _dataSource;

        public QueryableLeftJoinTest()
        {
            var dbName = $"kuery_leftjoin_test_{Guid.NewGuid():N}";
            _dataSource = System.IO.Path.Combine(AppContext.BaseDirectory, $"{dbName}.sqlite3");

            using var connection = CreateConnection();
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = @"
                CREATE TABLE [Order] (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    PlacedTime DATETIME NOT NULL
                );
                CREATE TABLE OrderLine (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    OrderId INTEGER NOT NULL,
                    ProductId INTEGER NOT NULL,
                    Quantity INTEGER NOT NULL,
                    UnitPrice DECIMAL NOT NULL,
                    Status INTEGER NOT NULL
                );";
            command.ExecuteNonQuery();
        }

        public void Dispose()
        {
            try { if (System.IO.File.Exists(_dataSource)) System.IO.File.Delete(_dataSource); }
            catch (System.IO.IOException) { }
        }

        private SqliteConnection CreateConnection()
        {
            var csb = new SqliteConnectionStringBuilder { DataSource = _dataSource };
            return new SqliteConnection(csb.ToString());
        }

        private SqliteConnection OpenNewConnection()
        {
            var c = CreateConnection();
            c.Open();
            return c;
        }

        private void SeedData()
        {
            using var conn = OpenNewConnection();
            conn.Insert(new Order { PlacedTime = new DateTime(2025, 1, 1) }); // Id=1, has lines
            conn.Insert(new Order { PlacedTime = new DateTime(2025, 2, 1) }); // Id=2, no lines
            conn.Insert(new OrderLine { OrderId = 1, ProductId = 10, Quantity = 2, UnitPrice = 100, Status = OrderLineStatus.Placed });
            conn.Insert(new OrderLine { OrderId = 1, ProductId = 20, Quantity = 1, UnitPrice = 200, Status = OrderLineStatus.Shipped });
        }

        [Fact]
        public void LeftJoinIncludesOrdersWithoutLines()
        {
            SeedData();
            using var conn = OpenNewConnection();

            var result = conn.Query<Order>()
                .GroupJoin(
                    conn.Query<OrderLine>(),
                    o => o.Id,
                    ol => ol.OrderId,
                    (o, ols) => new { o, ols })
                .SelectMany(
                    x => x.ols.DefaultIfEmpty(),
                    (x, ol) => new { x.o.Id, OlId = ol == null ? (int?)null : (int?)ol.Id })
                .ToList();

            // Order 1 has 2 lines → 2 rows; Order 2 has 0 lines → 1 row with null inner
            Assert.Equal(3, result.Count);

            var withoutLine = result.Where(r => r.OlId == null).ToList();
            Assert.Single(withoutLine);
            Assert.Equal(2, withoutLine[0].Id);
        }

        [Fact]
        public void LeftJoinAllMatchingReturnsCorrectCount()
        {
            SeedData();
            using var conn = OpenNewConnection();

            var result = conn.Query<Order>()
                .GroupJoin(
                    conn.Query<OrderLine>(),
                    o => o.Id,
                    ol => ol.OrderId,
                    (o, ols) => new { o, ols })
                .SelectMany(
                    x => x.ols.DefaultIfEmpty(),
                    (x, ol) => new { x.o.Id, x.o.PlacedTime, ProductId = ol == null ? (int?)null : (int?)ol.ProductId })
                .ToList();

            Assert.Equal(3, result.Count);

            // The two lines from Order 1
            Assert.Equal(2, result.Count(r => r.Id == 1));
            // The null row from Order 2
            Assert.Equal(1, result.Count(r => r.Id == 2 && r.ProductId == null));
        }

        [Fact]
        public void LeftJoinEmptyInnerTableReturnsAllOuter()
        {
            // Only insert orders, no lines
            using var conn = OpenNewConnection();
            conn.Insert(new Order { PlacedTime = new DateTime(2025, 1, 1) });
            conn.Insert(new Order { PlacedTime = new DateTime(2025, 2, 1) });

            var result = conn.Query<Order>()
                .GroupJoin(
                    conn.Query<OrderLine>(),
                    o => o.Id,
                    ol => ol.OrderId,
                    (o, ols) => new { o, ols })
                .SelectMany(
                    x => x.ols.DefaultIfEmpty(),
                    (x, ol) => new { x.o.Id })
                .ToList();

            Assert.Equal(2, result.Count);
        }

        [Fact]
        public void LeftJoinCountIncludesNullRows()
        {
            SeedData();
            using var conn = OpenNewConnection();

            var count = conn.Query<Order>()
                .GroupJoin(
                    conn.Query<OrderLine>(),
                    o => o.Id,
                    ol => ol.OrderId,
                    (o, ols) => new { o, ols })
                .SelectMany(
                    x => x.ols.DefaultIfEmpty(),
                    (x, ol) => new { x.o.Id })
                .Count();

            Assert.Equal(3, count);
        }

        [Fact]
        public async Task LeftJoinToListAsyncWorks()
        {
            SeedData();
            using var conn = OpenNewConnection();

            var result = await conn.Query<Order>()
                .GroupJoin(
                    conn.Query<OrderLine>(),
                    o => o.Id,
                    ol => ol.OrderId,
                    (o, ols) => new { o, ols })
                .SelectMany(
                    x => x.ols.DefaultIfEmpty(),
                    (x, ol) => new { x.o.Id })
                .ToListAsync();

            Assert.Equal(3, result.Count);
        }
    }

    // ---------------------------------------------------------------------------
    // Tests: Phase 1-2  Composite-key INNER JOIN
    // ---------------------------------------------------------------------------
    public class QueryableCompositeKeyJoinTest : IDisposable
    {
        readonly string _dataSource;

        public QueryableCompositeKeyJoinTest()
        {
            var dbName = $"kuery_compositekey_test_{Guid.NewGuid():N}";
            _dataSource = System.IO.Path.Combine(AppContext.BaseDirectory, $"{dbName}.sqlite3");

            using var connection = CreateConnection();
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = @"
                CREATE TABLE [Order] (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    PlacedTime DATETIME NOT NULL
                );
                CREATE TABLE OrderLine (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    OrderId INTEGER NOT NULL,
                    ProductId INTEGER NOT NULL,
                    Quantity INTEGER NOT NULL,
                    UnitPrice DECIMAL NOT NULL,
                    Status INTEGER NOT NULL
                );";
            command.ExecuteNonQuery();
        }

        public void Dispose()
        {
            try { if (System.IO.File.Exists(_dataSource)) System.IO.File.Delete(_dataSource); }
            catch (System.IO.IOException) { }
        }

        private SqliteConnection CreateConnection()
        {
            var csb = new SqliteConnectionStringBuilder { DataSource = _dataSource };
            return new SqliteConnection(csb.ToString());
        }

        private SqliteConnection OpenNewConnection()
        {
            var c = CreateConnection();
            c.Open();
            return c;
        }

        [Fact]
        public void CompositeKeyJoinFiltersCorrectly()
        {
            using var conn = OpenNewConnection();
            conn.Insert(new Order { PlacedTime = new DateTime(2025, 1, 1) }); // Id=1
            conn.Insert(new Order { PlacedTime = new DateTime(2025, 2, 1) }); // Id=2
            conn.Insert(new OrderLine { OrderId = 1, ProductId = 10, Quantity = 2, UnitPrice = 100, Status = OrderLineStatus.Placed });
            conn.Insert(new OrderLine { OrderId = 1, ProductId = 20, Quantity = 1, UnitPrice = 200, Status = OrderLineStatus.Placed });
            conn.Insert(new OrderLine { OrderId = 2, ProductId = 10, Quantity = 3, UnitPrice = 100, Status = OrderLineStatus.Placed });

            // Composite key: outer (Id, Id) = inner (OrderId, ProductId)
            // Matches only when Order.Id == OrderLine.OrderId AND Order.Id == OrderLine.ProductId.
            // ProductId is 10 or 20, so no Order.Id (1 or 2) matches → empty result.
            var result = conn.Query<Order>()
                .Join(
                    conn.Query<OrderLine>(),
                    o => new { A = o.Id, B = o.Id },
                    ol => new { A = ol.OrderId, B = ol.ProductId },
                    (o, ol) => new { o.Id, ol.Quantity })
                .ToList();

            Assert.Empty(result);
        }

        [Fact]
        public void CompositeKeyJoinReturnsMatchingRows()
        {
            using var conn = OpenNewConnection();
            conn.Insert(new Order { PlacedTime = new DateTime(2025, 1, 1) }); // Id=1

            // Row that satisfies both: OrderId=1 (= Order.Id) AND ProductId=1 (= Order.Id)
            conn.Insert(new OrderLine { OrderId = 1, ProductId = 1, Quantity = 5, UnitPrice = 50, Status = OrderLineStatus.Placed });
            // Row that satisfies only one: OrderId=1 (match) but ProductId=99 != 1
            conn.Insert(new OrderLine { OrderId = 1, ProductId = 99, Quantity = 3, UnitPrice = 30, Status = OrderLineStatus.Placed });

            // Composite: Order.Id = OrderLine.OrderId AND Order.Id = OrderLine.ProductId
            var result = conn.Query<Order>()
                .Join(
                    conn.Query<OrderLine>(),
                    o => new { A = o.Id, B = o.Id },
                    ol => new { A = ol.OrderId, B = ol.ProductId },
                    (o, ol) => new { o.Id, ol.Quantity })
                .ToList();

            Assert.Single(result);
            Assert.Equal(5, result[0].Quantity);
        }

        [Fact]
        public void CompositeKeyJoinCountIsCorrect()
        {
            using var conn = OpenNewConnection();
            conn.Insert(new Order { PlacedTime = new DateTime(2025, 1, 1) }); // Id=1
            conn.Insert(new Order { PlacedTime = new DateTime(2025, 2, 1) }); // Id=2

            // For Order.Id=1: ProductId=1 matches, ProductId=2 does not
            conn.Insert(new OrderLine { OrderId = 1, ProductId = 1, Quantity = 2, UnitPrice = 100, Status = OrderLineStatus.Placed });
            conn.Insert(new OrderLine { OrderId = 1, ProductId = 2, Quantity = 1, UnitPrice = 200, Status = OrderLineStatus.Placed });
            // For Order.Id=2: ProductId=2 matches, ProductId=1 does not
            conn.Insert(new OrderLine { OrderId = 2, ProductId = 2, Quantity = 3, UnitPrice = 50, Status = OrderLineStatus.Placed });
            conn.Insert(new OrderLine { OrderId = 2, ProductId = 1, Quantity = 4, UnitPrice = 75, Status = OrderLineStatus.Placed });

            var count = conn.Query<Order>()
                .Join(
                    conn.Query<OrderLine>(),
                    o => new { A = o.Id, B = o.Id },
                    ol => new { A = ol.OrderId, B = ol.ProductId },
                    (o, ol) => new { o.Id })
                .Count();

            // Order 1 matches with ProductId=1; Order 2 matches with ProductId=2 → 2 rows
            Assert.Equal(2, count);
        }
    }

    // ---------------------------------------------------------------------------
    // Tests: Phase 1-3  Multiple table JOIN (chained Join calls)
    // ---------------------------------------------------------------------------
    public class QueryableMultiJoinTest : IDisposable
    {
        readonly string _dataSource;

        public QueryableMultiJoinTest()
        {
            var dbName = $"kuery_multijoin_test_{Guid.NewGuid():N}";
            _dataSource = System.IO.Path.Combine(AppContext.BaseDirectory, $"{dbName}.sqlite3");

            using var connection = CreateConnection();
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = @"
                CREATE TABLE [Order] (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    PlacedTime DATETIME NOT NULL
                );
                CREATE TABLE OrderLine (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    OrderId INTEGER NOT NULL,
                    ProductId INTEGER NOT NULL,
                    Quantity INTEGER NOT NULL,
                    UnitPrice DECIMAL NOT NULL,
                    Status INTEGER NOT NULL
                );
                CREATE TABLE Product (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL,
                    Price DECIMAL NOT NULL,
                    TotalSales INTEGER NOT NULL
                );";
            command.ExecuteNonQuery();
        }

        public void Dispose()
        {
            try { if (System.IO.File.Exists(_dataSource)) System.IO.File.Delete(_dataSource); }
            catch (System.IO.IOException) { }
        }

        private SqliteConnection CreateConnection()
        {
            var csb = new SqliteConnectionStringBuilder { DataSource = _dataSource };
            return new SqliteConnection(csb.ToString());
        }

        private SqliteConnection OpenNewConnection()
        {
            var c = CreateConnection();
            c.Open();
            return c;
        }

        private void SeedData()
        {
            using var conn = OpenNewConnection();
            conn.Insert(new Product { Name = "Widget", Price = 100, TotalSales = 0 }); // Id=1
            conn.Insert(new Product { Name = "Gadget", Price = 200, TotalSales = 0 }); // Id=2
            conn.Insert(new Order { PlacedTime = new DateTime(2025, 1, 1) });           // Id=1
            conn.Insert(new Order { PlacedTime = new DateTime(2025, 2, 1) });           // Id=2
            conn.Insert(new OrderLine { OrderId = 1, ProductId = 1, Quantity = 2, UnitPrice = 100, Status = OrderLineStatus.Placed });
            conn.Insert(new OrderLine { OrderId = 1, ProductId = 2, Quantity = 1, UnitPrice = 200, Status = OrderLineStatus.Shipped });
            conn.Insert(new OrderLine { OrderId = 2, ProductId = 1, Quantity = 3, UnitPrice = 100, Status = OrderLineStatus.Placed });
        }

        [Fact]
        public void ThreeTableJoinReturnsCorrectCount()
        {
            SeedData();
            using var conn = OpenNewConnection();

            // Order INNER JOIN OrderLine ON Order.Id = OrderLine.OrderId
            //       INNER JOIN Product  ON OrderLine.ProductId = Product.Id
            var result = conn.Query<Order>()
                .Join(
                    conn.Query<OrderLine>(),
                    o => o.Id,
                    ol => ol.OrderId,
                    (o, ol) => new { o, ol })
                .Join(
                    conn.Query<Product>(),
                    x => x.ol.ProductId,
                    p => p.Id,
                    (x, p) => new { x.o.Id, x.ol.Quantity, p.Name })
                .ToList();

            Assert.Equal(3, result.Count);
        }

        [Fact]
        public void ThreeTableJoinValuesAreCorrect()
        {
            SeedData();
            using var conn = OpenNewConnection();

            var result = conn.Query<Order>()
                .Join(
                    conn.Query<OrderLine>(),
                    o => o.Id,
                    ol => ol.OrderId,
                    (o, ol) => new { o, ol })
                .Join(
                    conn.Query<Product>(),
                    x => x.ol.ProductId,
                    p => p.Id,
                    (x, p) => new { OrderId = x.o.Id, x.ol.Quantity, ProductName = p.Name })
                .ToList();

            Assert.Equal(3, result.Count);

            var row1 = result.First(r => r.OrderId == 1 && r.ProductName == "Widget");
            Assert.Equal(2, row1.Quantity);

            var row2 = result.First(r => r.OrderId == 1 && r.ProductName == "Gadget");
            Assert.Equal(1, row2.Quantity);

            var row3 = result.First(r => r.OrderId == 2 && r.ProductName == "Widget");
            Assert.Equal(3, row3.Quantity);
        }

        [Fact]
        public void ThreeTableJoinWithWhereBeforeFirstJoin()
        {
            SeedData();
            using var conn = OpenNewConnection();

            var result = conn.Query<Order>()
                .Where(o => o.Id == 1)
                .Join(
                    conn.Query<OrderLine>(),
                    o => o.Id,
                    ol => ol.OrderId,
                    (o, ol) => new { o, ol })
                .Join(
                    conn.Query<Product>(),
                    x => x.ol.ProductId,
                    p => p.Id,
                    (x, p) => new { OrderId = x.o.Id, x.ol.Quantity, ProductName = p.Name })
                .ToList();

            Assert.Equal(2, result.Count);
            Assert.All(result, r => Assert.Equal(1, r.OrderId));
        }

        [Fact]
        public void ThreeTableJoinCountIsCorrect()
        {
            SeedData();
            using var conn = OpenNewConnection();

            var count = conn.Query<Order>()
                .Join(
                    conn.Query<OrderLine>(),
                    o => o.Id,
                    ol => ol.OrderId,
                    (o, ol) => new { o, ol })
                .Join(
                    conn.Query<Product>(),
                    x => x.ol.ProductId,
                    p => p.Id,
                    (x, p) => new { x.o.Id })
                .Count();

            Assert.Equal(3, count);
        }

        [Fact]
        public void ThreeTableJoinFirstReturnsFirstRow()
        {
            SeedData();
            using var conn = OpenNewConnection();

            var first = conn.Query<Order>()
                .Join(
                    conn.Query<OrderLine>(),
                    o => o.Id,
                    ol => ol.OrderId,
                    (o, ol) => new { o, ol })
                .Join(
                    conn.Query<Product>(),
                    x => x.ol.ProductId,
                    p => p.Id,
                    (x, p) => new { OrderId = x.o.Id, ProductName = p.Name })
                .First();

            Assert.NotNull(first);
        }

        [Fact]
        public async Task ThreeTableJoinToListAsyncWorks()
        {
            SeedData();
            using var conn = OpenNewConnection();

            var result = await conn.Query<Order>()
                .Join(
                    conn.Query<OrderLine>(),
                    o => o.Id,
                    ol => ol.OrderId,
                    (o, ol) => new { o, ol })
                .Join(
                    conn.Query<Product>(),
                    x => x.ol.ProductId,
                    p => p.Id,
                    (x, p) => new { OrderId = x.o.Id, x.ol.Quantity, ProductName = p.Name })
                .ToListAsync();

            Assert.Equal(3, result.Count);
        }

        [Fact]
        public void ThreeTableJoinNoMatchReturnsEmpty()
        {
            using var conn = OpenNewConnection();
            // No data at all
            var result = conn.Query<Order>()
                .Join(
                    conn.Query<OrderLine>(),
                    o => o.Id,
                    ol => ol.OrderId,
                    (o, ol) => new { o, ol })
                .Join(
                    conn.Query<Product>(),
                    x => x.ol.ProductId,
                    p => p.Id,
                    (x, p) => new { x.o.Id })
                .ToList();

            Assert.Empty(result);
        }
    }
}
