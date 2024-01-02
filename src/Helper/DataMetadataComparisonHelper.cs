using DbSyncKit.Core.Comparer;
using DbSyncKit.Core.DataContract;
using DbSyncKit.Core.Enum;
using DbSyncKit.DB.Interface;
using DbSyncKit.DB.Manager;
using System.Collections.Concurrent;
using System.Reflection;

namespace DbSyncKit.Core.Helper
{
    /// <summary>
    /// Helper class for comparing metadata and data differences between two sets of data contracts.
    /// </summary>
    /// <typeparam name="T">Type of data contract implementing the IDataContractComparer interface.</typeparam>
    public class DataMetadataComparisonHelper<T> where T : IDataContractComparer
    {
        #region Public Methods

        /// <summary>
        /// Compares metadata and data differences between two sets of data contracts.
        /// </summary>
        /// <param name="sourceList">The source set of data contracts.</param>
        /// <param name="destinationList">The destination set of data contracts.</param>
        /// <param name="keyComparer">An instance of <see cref="KeyEqualityComparer{T}"/> used for key comparison.</param>
        /// <returns>A <see cref="Result{T}"/> object containing added, deleted, and edited data contracts, as well as data counts.</returns>
        public static Result<T> GetDifferences(HashSet<T> sourceList, HashSet<T> destinationList, KeyEqualityComparer<T> keyComparer, PropertyInfo[] CompariableProperties)
        {

            List<T> added = new List<T>();
            List<T> deleted = new List<T>();
            ConcurrentBag<(T edit, Dictionary<string, object> updatedProperties)> edited = new ConcurrentBag<(T, Dictionary<string, object>)>();

            // Identify added entries
            added.AddRange(sourceList.Except(destinationList, keyComparer));

            // Identify deleted entries
            deleted.AddRange(destinationList.Except(sourceList, keyComparer));

            // Identify edited entries
            var sourceKeyDictionary = sourceList
                .Except(added)
                .ToDictionary(row => GenerateCompositeKey(row, keyComparer.keyProperties), row => row);

            var destinationKeyDictionary = destinationList
                .Except(deleted)
                .ToDictionary(row => GenerateCompositeKey(row, keyComparer.keyProperties), row => row);

            Parallel.ForEach(sourceKeyDictionary, kvp =>
            {
                var sourceContract = kvp.Value;

                T? destinationContract;
                if (destinationKeyDictionary.TryGetValue(GenerateCompositeKey(sourceContract, keyComparer.keyProperties), out destinationContract))
                {
                    var (isEdited, updatedProperties) = GetEdited(sourceContract, destinationContract, CompariableProperties);

                    if (isEdited)
                    {
                        edited.Add((sourceContract, updatedProperties));
                    }
                }
            });


            var result = new Result<T>();
            result.Added = added;
            result.Deleted = deleted;
            result.Edited = edited.ToList();
            result.SourceDataCount = sourceList.Count;
            result.DestinaionDataCount = destinationList.Count;
            result.ResultChangeType = DetermineChangeType(result);
            return result;
        }

        #endregion

        #region Private Methods

        /// <summary>
        /// Compares two entities of type <typeparamref name="T"/> to identify if any properties
        /// have been edited during synchronization.
        /// </summary>
        /// <param name="source">The original entity before synchronization.</param>
        /// <param name="destination">The entity in the destination after synchronization.</param>
        /// <param name="compariableProperties">The properties to be compared for edits.</param>
        /// <returns>
        /// A tuple where:
        /// - <see cref="ValueTuple{T1,T2}.Item1"/> is a boolean indicating if any properties were edited.
        /// - <see cref="ValueTuple{T1,T2}.Item2"/> is a dictionary of updated properties for the edited entity.
        /// </returns>
        private static (bool isEdited, Dictionary<string, object> updatedProperties) GetEdited(T source, T destination, PropertyInfo[] compariableProperties)
        {
            Dictionary<string, object> updatedProperties = new();
            bool isEdited = false;
            if (source.Equals(destination))
            {
                return (isEdited, updatedProperties);
            }

            foreach (PropertyInfo prop in compariableProperties)
            {
                object sourceValue = prop.GetValue(source)!;
                object destinationValue = prop.GetValue(destination)!;

                // Compare values
                if (!EqualityComparer<object>.Default.Equals(sourceValue, destinationValue))
                {
                    isEdited = true;
                    updatedProperties[prop.Name] = sourceValue;
                }
            }

            return (isEdited, updatedProperties);
        }

        /// <summary>
        /// Generates a composite key based on specified properties for an entity of type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="dataContract">The entity for which the composite key is generated.</param>
        /// <param name="keyProperties">The properties used to create the composite key.</param>
        /// <returns>A string representing the composite key.</returns>
        private static string GenerateCompositeKey(T dataContract, PropertyInfo[] keyProperties)
        {
            var values = new string[keyProperties.Length];

            for (int i = 0; i < keyProperties.Length; i++)
            {
                values[i] = keyProperties[i].GetValue(dataContract)?.ToString()!;
            }

            return string.Join("_", values);
        }

        /// <summary>
        /// Determines the type of change that occurred during synchronization based on the differences in the result.
        /// </summary>
        /// <param name="result">The result object containing the differences between source and destination data.</param>
        /// <returns>The <see cref="ChangeType"/> indicating the type of change that occurred.</returns>
        private static ChangeType DetermineChangeType(Result<T> result)
        {
            // Tuple representing combinations of hasAdded, hasEdited, and hasDeleted
            var key = (result.Added?.Count > 0, result.Edited?.Count > 0, result.Deleted?.Count > 0);

            // Switch based on the combinations
            switch (key)
            {
                case (true, true, true):
                    // Added, Edited, and Deleted present
                    return ChangeType.All;

                case (true, false, false):
                    // Only Added present
                    return ChangeType.Added;

                case (true, true, false):
                    // Added and Edited present
                    return ChangeType.AddedWithEdited;

                case (false, true, false):
                    // Only Edited present
                    return ChangeType.Edited;

                case (false, true, true):
                    // Edited and Deleted present
                    return ChangeType.EditedWithDeleted;

                case (false, false, true):
                    // Only Deleted present
                    return ChangeType.Deleted;

                default:
                    // No significant changes
                    return ChangeType.None;
            }
        }
        #endregion

    }
}
