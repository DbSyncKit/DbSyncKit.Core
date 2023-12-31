﻿using DbSyncKit.Core.Comparer;
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
        /// Compares two sets of data entities and identifies the added, deleted, and edited entries.
        /// </summary>
        /// <param name="sourceList">The source set of data entities.</param>
        /// <param name="destinationList">The destination set of data entities.</param>
        /// <param name="keyComparer">An equality comparer for identifying key properties.</param>
        /// <param name="CompariableProperties">An array of PropertyInfo objects representing properties used for comparison.</param>
        /// <returns>A Result object containing the added, deleted, and edited entries.</returns>
        public static Result<T> GetDifferences(HashSet<T> sourceList, HashSet<T> destinationList, PropertyEqualityComparer<T> keyComparer, PropertyInfo[] CompariableProperties)
        {

            List<T> added = new List<T>();
            List<T> deleted = new List<T>();
            ConcurrentBag<(T edit, Dictionary<string, object> updatedProperties)> edited = new ConcurrentBag<(T, Dictionary<string, object>)>();
            PropertyEqualityComparer<T> CompariablePropertyComparer = new PropertyEqualityComparer<T>(CompariableProperties);

            // Identify added entries
            added.AddRange(sourceList.Except(destinationList, keyComparer));

            // Identify deleted entries
            deleted.AddRange(destinationList.Except(sourceList, keyComparer));

            // Identify edited entries
            var sourceKeyDictionary = sourceList
                .Except(added)
                .ToDictionary(row => GenerateCompositeKey(row, keyComparer.properties), row => row);

            var destinationKeyDictionary = destinationList
                .Except(deleted)
                .ToDictionary(row => GenerateCompositeKey(row, keyComparer.properties), row => row);

            Parallel.ForEach(sourceKeyDictionary, kvp =>
            {
                var sourceContract = kvp.Value;

                T? destinationContract;
                if (destinationKeyDictionary.TryGetValue(GenerateCompositeKey(sourceContract, keyComparer.properties), out destinationContract))
                {
                    var (isEdited, updatedProperties) = GetEdited(sourceContract, destinationContract, CompariablePropertyComparer);

                    if (isEdited)
                    {
                        edited.Add((sourceContract, updatedProperties)!);
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
        /// Compares two instances of the data contract and identifies the properties that have been edited.
        /// </summary>
        /// <param name="source">The source instance to compare.</param>
        /// <param name="destination">The destination instance to compare against.</param>
        /// <param name="comparablePropertyComparer">The comparer used to determine which properties are comparable.</param>
        /// <returns>
        /// A tuple indicating whether the instances are edited and a dictionary containing the names and values of the updated properties.
        /// If the instances are not edited, returns (false, null).
        /// </returns>
        private static (bool isEdited, Dictionary<string, object>? updatedProperties) GetEdited(T source, T destination, PropertyEqualityComparer<T> comparablePropertyComparer)
        {
            if(comparablePropertyComparer.Equals(source,destination))
            {
                return (false, null);
            }

            Dictionary<string, object> updatedProperties = new();
            foreach (PropertyInfo prop in comparablePropertyComparer.properties)
            {
                object sourceValue = prop.GetValue(source)!;
                object destinationValue = prop.GetValue(destination)!;

                // Compare values
                if (!System.Collections.Generic.EqualityComparer<object>.Default.Equals(sourceValue, destinationValue))
                {
                    updatedProperties[prop.Name] = sourceValue;
                }
            }

            return (true, updatedProperties);
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
