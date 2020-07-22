# Kuery

## TODO

- [x] `SQLiteConnection`
  - ~~`DatabasePath`~~
  - ~~`LibVersionNumber`~~
  - ~~`TimeExecution`~~
  - ~~`Trace`~~
  - ~~`Tracer`~~
  - ~~`StoreDateTimeAsTicks`~~
  - ~~`StoreTimeSpanAsTicks`~~
  - ~~`DateTimeStringFormat`~~
  - ~~`EnableWriteAheadLogging()`~~
  - ~~`EnableLoadExtension(bool)`~~
  - ~~`BusyTimeout`~~
  - ~~`TableMappings`~~
  - [x] `GetMapping<T>(CreateFlags)`
  - [x] `GetMapping(Type, CreateFlags)`
  - ~~`DropTable<T>()`~~
  - ~~`DropTable(TableMapping)`~~
  - ~~`CreateTable<T>(CreateFlags)`~~
  - ~~`CreateTable(Type, CreateFlags)`~~
  - ~~`CreateTables<T, T2>(CreateFlags)`~~
  - ~~`CreateTables<T, T2, T3>(CreateFlags)`~~
  - ~~`CreateTables<T, T2, T3, T4>(CreateFlags)`~~
  - ~~`CreateTables<T, T2, T3, T4, T5>(CreateFlags)`~~
  - ~~`CreateTables(CreateFlags, params Type[])`~~
  - ~~`CreateIndex(string, string, string[], bool)`~~
  - ~~`CreateIndex(string, string, string, bool)`~~
  - ~~`CreateIndex(string, string, bool)`~~
  - ~~`CreateIndex(string, string[], bool)`~~
  - ~~`CreateIndex<T>(Expression<Func<T, object>>, bool)`~~
  - ~~`GetTableInfo(string)`~~
  - ~~`CreateCommand(string, params object[])`~~
  - [x] `Execute(string, params object[])`
  - [x] `ExecuteScalar<T>(string, params object[])`
  - [x] `Query<T>(string, params object[])`
  - ~~`DefferredQuery<T>(string, params object[])`~~
  - [x] `Query(TableMapping, string, params object[])`
  - ~~`DefferredQuery(TableMapping, string, params object[])`~~
  - [x] `Table<T>()`
  - [x] `Get<T>(object)`
  - [x] `Get(object, TableMapping)`
  - [x] `Get<T>(Expression<Func<T, bool>>)`
  - [x] `Find<T>(object)`
  - [x] `Find(object, TableMapping)`
  - [x] `Find<T>(Expression<Func<T, bool>>)`
  - [x] `FindWithQuery(string, params object[])`
  - [x] `FindWithQuery(TableMapping, string, params object[])`
  - ~~`IsInTransaction`~~
  - ~~`BeginTransaction()`~~
  - ~~`SaveTransactionPoint()`~~
  - ~~`Rollback()`~~
  - ~~`RollbackTo(string)`~~
  - ~~`Release(string)`~~
  - ~~`Commit()`~~
  - ~~`RunInTransaction(Action)`~~
  - [x] `InsertAll(IEnumerable, bool)`
  - ~~`InsertAll(IEnumerable, string, bool)`~~
  - [x] `InsertAll(IEnumerable, Type, bool)`
  - [x] `Insert(object)`
  - [x] `InsertOrReplace(object)`
  - [x] `Insert(object, Type)`
  - [x] `InsertOrReplace(object, Type)`
  - ~~`Insert(object, string)`~~
  - ~~`Insert(object, string, Type)`~~
  - [x] `Update(object)`
  - [x] `Update(object, Type)`
  - [x] `UpdateAll(IEnumerable, bool)`
  - [x] `Delete(object)`
  - [x] `Delete<T>(object)`
  - [x] `Delete(object, TableMapping)`
  - ~~`DeleteAll<T>()`~~
  - ~~`DeleteAll(TableMapping)`~~
  - ~~`Backup(string, string)`~~
  - ~~`Dispose()`~~
  - ~~`Close()`~~
  - ~~`TableChanged`~~
- [x] `SQLiteAsyncConnection`
  - ~~`DatabasePath`~~
  - ~~`LibVersionNumber`~~
  - ~~`DateTimeStringFormat`~~
  - ~~`GetBusyTimeout()`~~
  - ~~`SetBusyTimeoutAsync(TimeSpan)`~~
  - ~~`EnableWriteAheadLoggingAsync()`~~
  - ~~`StoreDateTimeAsTicks`~~
  - ~~`StoreTimeSpanAsTicks`~~
  - ~~`Trace`~~
  - ~~`Tracer`~~
  - ~~`TimeExecution`~~
  - ~~`TableMappings`~~
  - ~~`ResetPool()`~~
  - ~~`GetConnection()`~~
  - ~~`CloseAsync()`~~
  - ~~`EnableLoadExtensionAsync(bool)`~~
  - ~~`CreateTableAsync<T>(CreateFlags)`~~
  - ~~`CreateTableAsync(Type, CreateFlags)`~~
  - ~~`CreateTablesAsync<T, T2>(CreateFlags)`~~
  - ~~`CreateTablesAsync<T, T2, T3>(CreateFlags)`~~
  - ~~`CreateTablesAsync<T, T2, T3, T4>(CreateFlags)`~~
  - ~~`CreateTablesAsync<T, T2, T3, T4, T5>(CreateFlags)`~~
  - ~~`CreateTablesAsync(CreateFlags, params Type[])`~~
  - ~~`DropTableAsync<T>()`~~
  - ~~`DropTableAsync(TableMapping)`~~
  - ~~`CreateIndexAsync(string, string, bool)`~~
  - ~~`CreateIndexAsync(string, string, string, bool)`~~
  - ~~`CreateIndexAsync(string, string[], bool)`~~
  - ~~`CreateIndexAsync(string, string, string[], bool)`~~
  - ~~`CreateIndexAsync<T>(Expression<Func<T, object>>, bool)`~~
  - [x] `InsertAsync(object)`
  - [x] `InsertAsync(object, Type)`
  - ~~`InsertAsync(object, string)`~~
  - ~~`InsertAsync(object, string, Type)`~~
  - [x] `InsertOrReplaceAsync(object)`
  - [x] `InsertOrReplaceAsync(object, Type)`
  - [x] `UpdateAsync(object)`
  - [x] `UpdateAsync(object, Type)`
  - [x] `UpdateAllAsync(IEnumerable, bool)`
  - [x] `DeleteAsync(object)`
  - [x] `DeleteAsync<T>(object)`
  - [x] `DeleteAsync<T>(object, TableMapping map)`
  - ~~`DeleteAllAsync<T>()`~~
  - ~~`DeleteAllAsync(TableMapping)`~~
  - ~~`BackupAsync(string, string)`~~
  - [x] `GetAsync<T>(object)`
  - [x] `GetAsync<T>(object, TableMapping)`
  - [x] `GetAsync<T>(Expression<Func<T, bool>>)`
  - [x] `FindAsync<T>(object)`
  - [x] `FindAsync<T>(object, TableMapping)`
  - [x] `FindAsync<T>(Expression<Func<T, bool>>)`
  - [x] `FindWithQueryAsync<T>(string, params object[])`
  - [x] `FindWithQueryAsync(TableMapping, string, params object[])`
  - ~~`GetMappingAsync(Type, CreateFlags)`~~
  - ~~`GetMappingAsync<T>(CreateFlags)`~~
  - ~~`GetTableInfoAsync(string)`~~
  - [x] `ExecuteAsync(string, params object[])`
  - [x] `InsertAllAsync(IEnumerable, bool)`
  - ~~`InsertAllAsync(IEnumerable, string, bool)`~~
  - [x] `InsertAllAsync(IEnumerable, Type, bool)`
  - ~~`RunIntransactionAsync(Action<SQLiteConnection>)`~~
  - [x] `Table()`
  - [x] `ExecuteScalarAsync<T>(string, params object[])`
  - [x] `QueryAsync<T>(string, params object[])`
  - [x] `QueryAsync(TableMapping, string, params object[])`
  - ~~`DefferredQueryAsync<T>(string, params object[])`~~
  - ~~`DefferredQueryAsync<T>(TableMapping, string, params object[])`~~
- [x] `AsyncTableQuery<T>`
  - [x] `Where(Expression<Func<T, bool>>)`
  - [x] `Skip(int)`
  - [x] `Take(int)`
  - [x] `OrderBy<U>(Expression<Func<T, U>>)`
  - [x] `OrderByDescending<U>(Expression<Func<T, U>>)`
  - [x] `ThenBy<U>(Expression<Func<T, U>>)`
  - [x] `ThenByDescending<U>(Expression<Func<T, U>>)`
  - [x] `ToListAsync()`
  - [x] `ToArrayAsync()`
  - [x] `CountAsync()`
  - [x] `CountAsync(Expression<Func<T, bool>>)`
  - [x] `ElementAtAsync(int)`
  - [x] `FirstAsync()`
  - [x] `FirstOrDefaultAsync()`
  - [x] `FirstAsync(Expression<Func<T, bool>>)`
  - [x] `FirstOrDefaultAsync(Expression<Func<T, bool>>)`
  - [x] `DeleteAsync(Expression<Func<T, bool>>)`
  - [x] `DeleteAsync()`
- [x] `TableQuery<T>`
  - ~~`Clone()`~~
  - [x] `Where(Expression<Func<T, bool>>)`
  - [x] `Delete()`
  - [x] `Delete(Expression<Func<T, bool>>)`
  - [x] `Take(int)`
  - [x] `Skip(int)`
  - [x] `ElementAt(int)`
  - [x] `Defferred()`
  - [x] `OrderBy<U>(Expression<Func<T, U>>)`
  - [x] `OrderByDescending<U>(Expression<Func<T, U>>)`
  - [x] `ThenBy<U>(Expression<Func<T, U>>)`
  - [x] `ThenByDescending<U>(Expression<Func<T, U>>)`
  - [x] `Count()`
  - [x] `Count(Expression<Func<T, bool>>)`
  - [x] `GetEnumerator()`
  - [x] `ToList()`
  - [x] `ToArray()`
  - [x] `First()`
  - [x] `FirstOrDefault()`
  - [x] `First(Expression<Func<T, bool>>)`
  - [x] `FirstOrDefault(Expression<Func<T, bool>>)`
- [ ] `Tests`
  - [ ] AsyncTests
  - ~~BackupTest~~
  - [x] BooleanTest
  - [ ] ByteArrayTest
  - ~~CollateTest~~
  - ~~ConcurrencyTest~~
  - [x] ContainsTest
  - ~~CreateTableImplicitTest~~
  - ~~CreateTableTest~~
  - [ ] DateTimeOffsetTest
  - [x] DateTimeTest
  - [x] DeleteTest
  - ~~DropTableTest~~
  - [ ] EnumCacheTest
  - [ ] EnumNullableTest
  - [ ] EnumTest
  - [ ] EqualsTest
  - [x] GuidTest
  - [ ] InheritanceTest
  - [ ] InsertTest
  - ~~JoinTest~~
  - [ ] LinqTest
  - [ ] MappingTest
  - ~~MigrationTest~~
  - [ ] NotNullAttributeTest
  - [ ] NullableTest
  - ~~OpenTest~~
  - [x] QueryTest
  - ~~ReadmeTest~~
  - [x] ScalarTest
  - [x] SkipTest
  - ~~SQLCipherTest~~
  - [x] StringQueryTest
  - ~~TableChangedTest~~
  - [ ] TimeSpanTest
  - [ ] TransactionTest
  - [ ] UnicodeTest
  - [ ] UniqueTest

