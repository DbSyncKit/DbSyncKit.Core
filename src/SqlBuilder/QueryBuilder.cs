using DbSyncKit.Core.DataContract;
using DbSyncKit.DB.Helper;
using DbSyncKit.DB.Interface;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbSyncKit.Core.SqlBuilder
{
    /// <summary>
    /// Helper class for building SQL queries.
    /// </summary>
    /// <remarks>
    /// The <see cref="QueryBuilder"/> class extends the functionality of <see cref="QueryHelper"/> to provide methods for constructing SQL queries.
    /// It aids in the generation of queries for various database operations such as SELECT, INSERT, UPDATE, and DELETE.
    /// </remarks>
    public class QueryBuilder: QueryHelper
    {
        /// <summary>
        /// Generates SQL queries for synchronizing data based on the provided <seealso cref="Result{T}"/> object.
        /// </summary>
        /// <param name="result">The <seealso cref="Result{T}"/> object containing added, deleted, and edited entries.</param>
        /// <param name="QueryGeneratorManager">The <seealso cref="IQueryGenerator"/> instance used for generating SQL queries.</param>
        /// <param name="BatchSize">The size of query batches. Default is 20.</param>
        /// <returns>
        /// A string containing SQL queries for inserting, deleting, and updating data based on the provided <seealso cref="Result{T}"/> object.
        /// </returns>
        /// <exception cref="InvalidOperationException">Thrown when the <seealso cref="IQueryGenerator"/> instance is missing.</exception>
        /// <remarks>
        /// This method generates SQL queries for insert, delete, and update operations based on the provided <seealso cref="Result{T}"/> object.
        /// It uses the specified <seealso cref="IQueryGenerator"/> instance to generate SQL statements.
        /// The queries are separated into batches using the specified batch size.
        /// </remarks>
        public string GetSqlQueryForSyncData<T>(Result<T> result, IQueryGenerator QueryGeneratorManager, int BatchSize = 20)
        {
            if (QueryGeneratorManager == null)
            {
                throw new InvalidOperationException("The IQueryGenerator instance is missing.");
            }

            string batchStatement = QueryGeneratorManager.GenerateBatchSeparator();
            var query = new StringBuilder();
            var TableName = GetTableName<T>();
            var keyColumns = GetKeyColumns<T>();
            var excludedColumns = GetExcludedColumns<T>();

            query.AppendLine(QueryGeneratorManager.GenerateComment("==============" + TableName + "=============="));
            query.AppendLine(QueryGeneratorManager.GenerateComment("==============Insert==============="));

            for (int i = 0; i < result.Added.Count; i++)
            {
                query.AppendLine(QueryGeneratorManager.GenerateInsertQuery(result.Added[i], keyColumns, excludedColumns));
                if (i != 0 && i % BatchSize == 0)
                    query.AppendLine(batchStatement);
            }
            if (result.Added.Count > 0 && result.Added.Count % BatchSize != 0) query.AppendLine(batchStatement);
            query.AppendLine(QueryGeneratorManager.GenerateComment("==============Delete==============="));


            for (int i = 0; i < result.Deleted.Count; i++)
            {
                query.AppendLine(QueryGeneratorManager.GenerateDeleteQuery(result.Deleted[i], keyColumns));
                if (i != 0 && i % BatchSize == 0)
                    query.AppendLine(batchStatement);

            }

            if (result.Deleted.Count > 0 && result.Deleted.Count % BatchSize != 0) query.AppendLine(batchStatement);
            query.AppendLine(QueryGeneratorManager.GenerateComment("==============Update==============="));

            for (int i = 0; i < result.EditedDetailed.Count; i++)
            {
                query.AppendLine(QueryGeneratorManager.GenerateUpdateQuery<T>(result.EditedDetailed[i].sourceContract, keyColumns, excludedColumns, result.EditedDetailed[i].editedProperties));
                if (i != 0 && i % BatchSize == 0)
                    query.AppendLine(batchStatement);
            }

            if (result.EditedDetailed.Count > 0 && result.EditedDetailed.Count % BatchSize != 0) query.AppendLine(batchStatement);

            return query.ToString();
        }
    }
}
