using DbSyncKit.Core.Comparer;
using DbSyncKit.DB;
using DbSyncKit.DB.Factory;
using DbSyncKit.DB.Interface;

namespace DbSyncKit.Core.Fetcher
{
    /// <summary>
    /// Utility class for fetching data from a database using data contracts.
    /// </summary>
    public class DataContractFetcher
    {
        #region Properties

        /// <summary>
        /// Gets the query generator factory instance used for creating query generators.
        /// </summary>
        private readonly QueryGeneratorFactory Factory;

        /// <summary>
        /// Gets or sets the QueryGenerationManager instance for generating queries for the destination database.
        /// </summary>
        /// <remarks>
        /// This property is set by the <see cref="RetrieveDataFromDatabases{T}(IDatabase, IDatabase, string, List{string}, PropertyEqualityComparer{T}, out HashSet{T}, out HashSet{T})"/> method.
        /// It allows the reuse of the QueryGenerationManager instance when the source and destination databases share the same provider.
        /// </remarks>
        public IQueryGenerator? DestinationQueryGenerationManager { get; private set; }

        #endregion

        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="DataContractFetcher"/> class with a custom query generator factory.
        /// </summary>
        /// <param name="factory">The custom query generator factory instance.</param>
        public DataContractFetcher(QueryGeneratorFactory factory)
        {
            Factory = factory;
        }

        #endregion

        #region Methods

        /// <summary>
        /// Retrieves data from source and destination databases for a specified table and column list, using a specified data contract type.
        /// </summary>
        /// <param name="source">The source database.</param>
        /// <param name="destination">The destination database.</param>
        /// <param name="tableName">The name of the table from which to retrieve data.</param>
        /// <param name="ColumnList">A list of column names to retrieve from the table.</param>
        /// <param name="ComparablePropertyEqualityComparer">An equality comparer for identifying properties used in data comparison.</param>
        /// <param name="sourceList">An output parameter that receives the retrieved data from the source database.</param>
        /// <param name="destinationList">An output parameter that receives the retrieved data from the destination database.</param>
        /// <remarks>
        /// The method uses a QueryGenerationManager to generate queries for retrieving data based on the specified table and columns.
        /// If the providers of the source and destination databases differ, a new QueryGenerationManager is created for the destination database.
        /// </remarks>
        public void RetrieveDataFromDatabases<T>(
            IDatabase source, 
            IDatabase destination, 
            string tableName, 
            List<string> ColumnList, 
            PropertyEqualityComparer<T> ComparablePropertyEqualityComparer, 
            out HashSet<T> sourceList, 
            out HashSet<T> destinationList)
        {
            var sourceQueryGenerationManager = new QueryGenerationManager(Factory.GetQueryGenerator(source.Provider));
            sourceList = GetDataFromDatabase<T>(tableName, source, sourceQueryGenerationManager, ColumnList, ComparablePropertyEqualityComparer);

            if (source.Provider != destination.Provider)
            {
                sourceQueryGenerationManager.Dispose();
                DestinationQueryGenerationManager = new QueryGenerationManager(Factory.GetQueryGenerator(destination.Provider));
            }
            else
            {
                DestinationQueryGenerationManager = sourceQueryGenerationManager;
            }

            destinationList = GetDataFromDatabase<T>(tableName, destination, DestinationQueryGenerationManager, ColumnList, ComparablePropertyEqualityComparer);
        }

        /// <summary>
        /// Retrieves data from a database for a specified table, columns, and data contract type.
        /// </summary>
        /// <param name="tableName">The name of the table from which to retrieve data.</param>
        /// <param name="connection">The database connection.</param>
        /// <param name="manager">The query generation manager for creating the SELECT query.</param>
        /// <param name="columns">A list of column names to retrieve from the table.</param>
        /// <param name="ComparablePropertyEqualityComparer">An equality comparer for identifying properties used in data comparison.</param>
        /// <returns>
        /// A HashSet of data entities of type T retrieved from the specified table and columns in the database.
        /// </returns>
        /// <remarks>
        /// The method generates a SELECT query using the provided query generation manager and executes it using a DatabaseManager.
        /// The resulting data is converted to a HashSet using the specified property equality comparer for data comparison.
        /// </remarks>
        public HashSet<T> GetDataFromDatabase<T>(
            string tableName,
            IDatabase connection,
            IQueryGenerator manager,
            List<string> columns,
            PropertyEqualityComparer<T> ComparablePropertyEqualityComparer)
        {
            var query = manager.GenerateSelectQuery<T>(tableName, columns, string.Empty);

            using var DBManager = new DatabaseManager<IDatabase>(connection);
                return DBManager.ExecuteQuery<T>(query, tableName).ToHashSet(ComparablePropertyEqualityComparer);
        }

        #endregion
    }
}
