using DbSyncKit.Core.DataContract;
using DbSyncKit.Core.Enum;
using DbSyncKit.Core.Helper;
using DbSyncKit.Core.SqlBuilder;
using DbSyncKit.DB.Comparer;
using DbSyncKit.DB.Factory;
using DbSyncKit.DB.Helper;
using DbSyncKit.DB.Interface;
using DbSyncKit.DB.Fetcher;
using System.Reflection;

namespace DbSyncKit.Core
{
    /// <summary>
    /// Manages the synchronization of data between source and destination databases.
    /// </summary>
    public class Synchronization : QueryHelper
    {
        #region Decleration

        /// <summary>
        /// Gets or sets the instance of <see cref="DataContractFetcher"/> for fetching data using data contracts.
        /// </summary>
        /// <remarks>
        /// The <see cref="DataContractFetcher"/> instance provides methods for retrieving data from a database using data contracts.
        /// It is used to fetch and work with data entities during synchronization processes.
        /// </remarks>
        public DataContractFetcher ContractFetcher { get; private set; }

        /// <summary>
        /// Gets or sets the instance of <see cref="SqlBuilder.QueryBuilder"/> for building SQL queries.
        /// </summary>
        /// <remarks>
        /// The <see cref="SqlBuilder.QueryBuilder"/> instance extends the functionality of <see cref="QueryHelper"/> to aid in constructing SQL queries.
        /// It provides methods for building queries related to various database operations, including SELECT, INSERT, UPDATE, and DELETE.
        /// </remarks>
        public QueryBuilder QueryBuilder { get; private set; }

        /// <summary>
        /// Gets or sets the instance of <see cref="DataContractMismatchIdentifier"/> used for identifying mismatches
        /// during the synchronization process.
        /// </summary>
        public DataContractMismatchIdentifier MismatchIdentifier { get; private set; }

        #endregion

        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="Synchronization"/> class with default instances of <see cref="DataContractFetcher"/> and <see cref="QueryBuilder"/>.
        /// </summary>
        /// <remarks>
        /// This constructor creates a new <see cref="Synchronization"/> instance with default instances of <see cref="DataContractFetcher"/> and <see cref="QueryBuilder"/>.
        /// These instances are used for fetching data using data contracts and building SQL queries, respectively.
        /// </remarks>
        public Synchronization()
        {
            ContractFetcher = new DataContractFetcher(new QueryGeneratorFactory());
            QueryBuilder = new QueryBuilder();
            MismatchIdentifier = new DataContractMismatchIdentifier();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Synchronization"/> class with custom instances of <see cref="DataContractFetcher"/>,
        /// <see cref="QueryBuilder"/>, and <see cref="DataContractMismatchIdentifier"/>.
        /// </summary>
        /// <param name="fetcher">The custom <see cref="DataContractFetcher"/> instance to use for fetching data.</param>
        /// <param name="queryBuilder">The custom <see cref="QueryBuilder"/> instance to use for building SQL queries.</param>
        /// <param name="mismatchIdentifier">The custom <see cref="DataContractMismatchIdentifier"/> instance to use for identifying mismatches during synchronization.</param>
        /// <remarks>
        /// This constructor creates a new <see cref="Synchronization"/> instance with custom instances of <see cref="DataContractFetcher"/>,
        /// <see cref="QueryBuilder"/>, and <see cref="DataContractMismatchIdentifier"/>.
        /// It allows the use of specific instances for fetching data using data contracts, building SQL queries, and identifying mismatches during synchronization.
        /// </remarks>
        public Synchronization(DataContractFetcher fetcher, QueryBuilder queryBuilder, DataContractMismatchIdentifier mismatchIdentifier)
        {
            ContractFetcher = fetcher;
            QueryBuilder = queryBuilder;
            MismatchIdentifier = mismatchIdentifier;
        }


        #endregion

        #region Public Methods


        /// <summary>
        /// Synchronizes data of a specific type between source and destination databases.
        /// </summary>
        /// <typeparam name="T">The type of data entities to synchronize.</typeparam>
        /// <param name="source">The source database.</param>
        /// <param name="destination">The destination database.</param>
        /// <param name="filterDataCallback">A callback function for filtering data before synchronization.</param>
        /// <param name="direction">Specifies the direction of synchronization. Default is SourceToDestination.</param>
        /// <returns>A result object containing the differences between source and destination data.</returns>
        /// <remarks>
        /// This method synchronizes data between source and destination databases of a specific type. 
        /// It retrieves data from both databases, applies optional filtering using the provided callback function,
        /// and then compares the data to identify differences. The direction of synchronization determines 
        /// the comparison direction between source and destination databases.
        /// </remarks>
        public Result<T> SyncData<T>(IDatabase source, IDatabase destination, FilterCallback<T>? filterDataCallback, Direction direction = Direction.SourceToDestination)
        {
            #region Properties
            string tableName = GetTableName<T>();

            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentNullException(tableName, "Table Name Cannot be null");

            List<string> excludedProperty = GetExcludedColumns<T>();
            List<string> ColumnList = GetAllColumns<T>().Except(excludedProperty).ToList();
            PropertyInfo[] ComparableProperties = GetComparableProperties<T>();
            PropertyInfo[] keyProperties = GetKeyProperties<T>();
            PropertyEqualityComparer<T> keyEqualityComparer = new(keyProperties);
            PropertyEqualityComparer<T> ComparablePropertyEqualityComparer = new(ComparableProperties);
            HashSet<T> sourceList, destinationList;

            #endregion

            SwapDatabasesIfNeeded(ref source, ref destination, direction);

            ContractFetcher.RetrieveDataFromDatabases(source, destination, tableName, ColumnList, ComparablePropertyEqualityComparer, filterDataCallback, out sourceList, out destinationList);

            return MismatchIdentifier.GetDifferences(sourceList, destinationList, keyEqualityComparer, ComparablePropertyEqualityComparer);
        }


        #endregion

        #region Private Methods

        private static void SwapDatabasesIfNeeded(ref IDatabase source, ref IDatabase destination, Direction direction)
        {
            switch (direction)
            {
                case Direction.SourceToDestination:
                    return;

                case Direction.DestinationToSource:
                    IDatabase temp;
                    temp = source;
                    source = destination;
                    destination = temp;
                    return;
                default:
                    throw new NotImplementedException();
            }
        }

        #endregion
    }
}
