using DbSyncKit.Core.Comparer;
using DbSyncKit.Core.DataContract;
using DbSyncKit.Core.Enum;
using DbSyncKit.Core.Helper;
using DbSyncKit.DB;
using DbSyncKit.DB.Factory;
using DbSyncKit.DB.Helper;
using DbSyncKit.DB.Interface;
using System.Reflection;
using System.Text;

namespace DbSyncKit.Core
{
    /// <summary>
    /// Manages the synchronization of data between source and destination databases.
    /// </summary>
    public class Synchronization : QueryHelper
    {
        #region Decleration

        /// <summary>
        /// Gets or sets the IQueryGenerator instance for the destination database.
        /// </summary>
        public IQueryGenerator? destinationQueryGenerationManager { get; private set; }

        #endregion

        #region Constructor
        /// <summary>
        /// Initializes a new instance of the <see cref="Synchronization"/> class.
        /// </summary>
        public Synchronization()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Synchronization"/> class with specified IQueryGenerator instances.
        /// </summary>
        /// <param name="destination">The IQueryGenerator for the destination database.</param>
        public Synchronization(IQueryGenerator destination)
        {
            destinationQueryGenerationManager = destination;
        }

        #endregion

        #region Public Methods


        /// <summary>
        /// Synchronizes data of a specific type between source and destination databases.
        /// </summary>
        /// <typeparam name="T">The type of entity that implements IDataContractComparer.</typeparam>
        /// <param name="source">The source database.</param>
        /// <param name="destination">The destination database.</param>
        /// <param name="direction">Represents Which Direction to compare db</param>
        /// <returns>A result object containing the differences between source and destination data.</returns>
        public Result<T> SyncData<T>(IDatabase source, IDatabase destination, Direction direction = Direction.SourceToDestination) where T : IDataContractComparer
        {
            #region Properties
            string tableName = GetTableName<T>();

            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentNullException(tableName, "Table Name Cannot be null");

            List<string> excludedProperty = GetExcludedColumns<T>();
            List<string> ColumnList = GetAllColumns<T>().Except(excludedProperty).ToList();
            PropertyInfo[] ComparableProperties = GetComparableProperties<T>();
            PropertyInfo[] keyProperties = GetKeyProperties<T>();
            KeyEqualityComparer<T> keyEqualityComparer = new KeyEqualityComparer<T>(ComparableProperties, keyProperties);
            HashSet<T> sourceList, destinationList;

            #endregion

            SwapDatabasesIfNeeded(ref source, ref destination, direction);

            RetrieveDataFromDatabases(source, destination, tableName, ColumnList, keyEqualityComparer, out sourceList, out destinationList);

            return GetDifferences(sourceList, destinationList, keyEqualityComparer);
        }

        /// <summary>
        /// Retrieves data from two databases, <paramref name="source"/> and <paramref name="destination"/>,
        /// for objects of type <typeparamref name="T"/> based on the specified <paramref name="tableName"/> and column list.
        /// The retrieved data is stored in the output parameters <paramref name="sourceList"/> and <paramref name="destinationList"/>.
        /// </summary>
        /// <typeparam name="T">The type of objects to retrieve from the databases. Must implement <see cref="IDataContractComparer"/>.</typeparam>
        /// <param name="source">The source database from which to retrieve data.</param>
        /// <param name="destination">The destination database from which to retrieve data.</param>
        /// <param name="tableName">The name of the table for which to retrieve data.</param>
        /// <param name="ColumnList">The list of columns to retrieve from the table.</param>
        /// <param name="keyEqualityComparer">An instance of <see cref="KeyEqualityComparer{T}"/> used for data comparison.</param>
        /// <param name="sourceList">Output parameter for the retrieved data from the source database.</param>
        /// <param name="destinationList">Output parameter for the retrieved data from the destination database.</param>
        public void RetrieveDataFromDatabases<T>(IDatabase source, IDatabase destination, string tableName, List<string> ColumnList, KeyEqualityComparer<T> keyEqualityComparer, out HashSet<T> sourceList, out HashSet<T> destinationList) where T : IDataContractComparer
        {
            var sourceQueryGenerationManager = new QueryGenerationManager(QueryGeneratorFactory.GetQueryGenerator(source.Provider));
            sourceList = GetDataFromDatabase<T>(tableName, source, sourceQueryGenerationManager, ColumnList, keyEqualityComparer);

            if (source.Provider != destination.Provider)
            {
                sourceQueryGenerationManager.Dispose();
                destinationQueryGenerationManager = new QueryGenerationManager(QueryGeneratorFactory.GetQueryGenerator(destination.Provider));
            }
            else
            {
                destinationQueryGenerationManager = sourceQueryGenerationManager;
            }

            destinationList = GetDataFromDatabase<T>(tableName, destination, destinationQueryGenerationManager, ColumnList, keyEqualityComparer);
        }

        /// <summary>
        /// Retrieves data of type <typeparamref name="T"/> from the specified <paramref name="connection"/> using the provided <paramref name="manager"/>.
        /// The data is retrieved from the table specified by <paramref name="tableName"/> and the list of columns in <paramref name="columns"/>.
        /// The <paramref name="keyEqualityComparer"/> is used for data comparison, and the retrieved data is returned as a <see cref="HashSet{T}"/>.
        /// </summary>
        /// <typeparam name="T">The type of objects to retrieve. Must implement <see cref="IDataContractComparer"/>.</typeparam>
        /// <param name="tableName">The name of the table for which to retrieve data.</param>
        /// <param name="connection">The database connection from which to retrieve data.</param>
        /// <param name="manager">The query generation manager to use for generating the select query.</param>
        /// <param name="columns">The list of columns to retrieve from the table.</param>
        /// <param name="keyEqualityComparer">An instance of <see cref="KeyEqualityComparer{T}"/> used for data comparison.</param>
        /// <returns>A <see cref="HashSet{T}"/> containing the retrieved data.</returns>
        public HashSet<T> GetDataFromDatabase<T>(string tableName, IDatabase connection, IQueryGenerator manager, List<string> columns, KeyEqualityComparer<T> keyEqualityComparer) where T : IDataContractComparer
        {
            var query = manager.GenerateSelectQuery<T>(tableName, columns, string.Empty);

            using (var DBManager = new DatabaseManager<IDatabase>(connection))
            {
                return DBManager.ExecuteQuery<T>(query, tableName).ToHashSet(keyEqualityComparer);
            }
        }

        /// <summary>
        /// Compares the data in the <paramref name="sourceList"/> and <paramref name="destinationList"/> HashSet instances
        /// using the specified <paramref name="keyEqualityComparer"/> to determine key equality.
        /// Returns a <see cref="Result{T}"/> containing the differences between the two sets of data.
        /// </summary>
        /// <typeparam name="T">The type of objects being compared. Must implement <see cref="IDataContractComparer"/>.</typeparam>
        /// <param name="sourceList">The HashSet containing data from the source.</param>
        /// <param name="destinationList">The HashSet containing data from the destination.</param>
        /// <param name="keyEqualityComparer">An instance of <see cref="KeyEqualityComparer{T}"/> used for key comparison.</param>
        /// <returns>A <see cref="Result{T}"/> containing the differences between the source and destination data.</returns>
        public Result<T> GetDifferences<T>(HashSet<T> sourceList, HashSet<T> destinationList, KeyEqualityComparer<T> keyEqualityComparer) where T : IDataContractComparer
        {
            return DataMetadataComparisonHelper<T>.GetDifferences(sourceList, destinationList, keyEqualityComparer);
        }



        /// <summary>
        /// Generates SQL queries for synchronizing data based on the differences identified.
        /// </summary>
        /// <typeparam name="T">The type of entity that implements IDataContractComparer.</typeparam>
        /// <param name="result">The result object containing the differences between source and destination data.</param>
        /// <param name="BatchSize">The size of each batch for SQL statements (default is 20).</param>
        /// <returns>A string representing the generated SQL queries for synchronization.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the IQueryGenerator instance is missing.
        /// </exception>
        public string GetSqlQueryForSyncData<T>(Result<T> result,int BatchSize = 20) where T : IDataContractComparer
        {
            if (destinationQueryGenerationManager == null)
            {
                throw new InvalidOperationException("The IQueryGenerator instance is missing.");
            }

            string batchStatement = destinationQueryGenerationManager.GenerateBatchSeparator();
            var query = new StringBuilder();
            var TableName = GetTableName<T>();
            var keyColumns = GetKeyColumns<T>();
            var excludedColumns = GetExcludedColumns<T>();

            query.AppendLine(destinationQueryGenerationManager.GenerateComment("==============" + TableName + "=============="));
            query.AppendLine(destinationQueryGenerationManager.GenerateComment("==============Insert==============="));

            for (int i = 0; i < result.Added.Count; i++)
            {
                query.AppendLine(destinationQueryGenerationManager.GenerateInsertQuery(result.Added[i], keyColumns, excludedColumns));
                if (i != 0 && i % BatchSize == 0)
                    query.AppendLine(batchStatement);
            }
            if (result.Added.Count > 0 && result.Added.Count % BatchSize != 0) query.AppendLine(batchStatement);
            query.AppendLine(destinationQueryGenerationManager.GenerateComment("==============Delete==============="));


            for (int i = 0; i < result.Deleted.Count; i++)
            {
                query.AppendLine(destinationQueryGenerationManager.GenerateDeleteQuery(result.Deleted[i], keyColumns));
                if (i != 0 && i % BatchSize == 0)
                    query.AppendLine(batchStatement);

            }

            if (result.Deleted.Count > 0 && result.Deleted.Count % BatchSize != 0) query.AppendLine(batchStatement);
            query.AppendLine(destinationQueryGenerationManager.GenerateComment("==============Update==============="));

            for (int i = 0; i < result.Edited.Count; i++)
            {
                query.AppendLine(destinationQueryGenerationManager.GenerateUpdateQuery<T>(result.Edited[i].sourceContract, keyColumns, excludedColumns, result.Edited[i].editedProperties));
                if (i != 0 && i % BatchSize == 0)
                    query.AppendLine(batchStatement);
            }

            if (result.Edited.Count > 0 && result.Edited.Count % BatchSize != 0) query.AppendLine(batchStatement);

            return query.ToString();
        }

        /// <summary>
        /// Generates SQL queries for synchronizing data based on the differences identified, using the provided
        /// IQueryGenerator instance for query generation.
        /// </summary>
        /// <typeparam name="T">The type of entity that implements IDataContractComparer.</typeparam>
        /// <param name="result">The result object containing the differences between source and destination data.</param>
        /// <param name="QueryGeneration">The IQueryGenerator instance for query generation.</param>
        /// <param name="BatchSize">The size of each batch for SQL statements (default is 20).</param>
        /// <returns>A string representing the generated SQL queries for synchronization.</returns>
        public string GetSqlQueryForSyncData<T>(Result<T> result, IQueryGenerator QueryGeneration, int BatchSize = 20) where T : IDataContractComparer
        {
            destinationQueryGenerationManager = QueryGeneration;

            return GetSqlQueryForSyncData<T>(result, BatchSize);
        }


        #endregion

        #region Private Methods

        private static void SwapDatabasesIfNeeded(ref IDatabase source, ref IDatabase destination, Direction direction)
        {
            switch (direction)
            {
                case Direction.SourceToDestination:
                    break;

                case Direction.DestinationToSource:
                    IDatabase temp;
                    temp = source;
                    source = destination;
                    destination = temp;
                    break;
                default:
                    throw new NotImplementedException();
            }
        }

        #endregion
    }
}
