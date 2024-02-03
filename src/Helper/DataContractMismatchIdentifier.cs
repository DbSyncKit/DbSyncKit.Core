using DbSyncKit.Core.Comparer;
using DbSyncKit.Core.DataContract;
using DbSyncKit.Core.Enum;
using DbSyncKit.DB.Interface;
using System.Reflection;

namespace DbSyncKit.Core.Helper
{
    /// <summary>
    /// Identifies mismatches and differences between data contracts.
    /// </summary>
    public class DataContractMismatchIdentifier
    {
        #region Public Methods

        /// <summary>
        /// Compares two sets of data entities and identifies the added, deleted, and edited entries.
        /// </summary>
        /// <param name="sourceList">The source set of data entities.</param>
        /// <param name="destinationList">The destination set of data entities.</param>
        /// <param name="keyComparer">An equality comparer for identifying key properties.</param>
        /// <param name="CompariablePropertyComparer">An equality comparer for identifying properties used in the comparison.</param>
        /// <param name="DetailedComparison">Specifies whether to include detailed comparison results for each property change. Default is true.</param>
        /// <returns>
        /// A <seealso cref="Result{T}"/> object containing the added, deleted, and edited entries. 
        /// For detailed comparisons, it includes information about individual property changes in the <seealso cref="Result{T}.EditedDetailed"/> property; 
        /// otherwise, edited entries are listed in the <seealso cref="Result{T}.Edited"/> property.
        /// </returns>
        public Result<T> GetDifferences<T>(
            HashSet<T> sourceList,
            HashSet<T> destinationList,
            PropertyEqualityComparer<T> keyComparer,
            PropertyEqualityComparer<T> CompariablePropertyComparer,
            bool DetailedComparison = true)
        {
            var result = new Result<T>();

            // Identify added entries mixed with edited
            HashSet<T> Added = sourceList.Except(destinationList, CompariablePropertyComparer).ToHashSet(keyComparer);

            // Identify deleted entries  mixed with edited
            HashSet<T> Removed = destinationList.Except(sourceList, CompariablePropertyComparer).ToHashSet(keyComparer);

            // Get Common between added and deleted
            HashSet<T> Altered = Added.Intersect(Removed,keyComparer).ToHashSet(keyComparer);

            // remove the common from added and deleted
            result.Added = Added.Except(Altered, keyComparer).ToList();
            result.Deleted = Removed.Except(Altered, keyComparer).ToList();

            if (DetailedComparison)
            { 
                result.EditedDetailed = GetEdited(Added,Removed,keyComparer,CompariablePropertyComparer);
            }
            else
            {
                result.Edited = Altered.ToList();
            }

            result.SourceDataCount = sourceList.Count;
            result.DestinaionDataCount = destinationList.Count;
            result.ResultChangeType = DetermineChangeType(result,DetailedComparison);
            return result;
        }

        /// <summary>
        /// Determines the overall change type based on the presence of added, edited, and deleted entities in the synchronization <seealso cref="Result{T}"/>.
        /// </summary>
        /// <param name="result">The synchronization <seealso cref="Result{T}"/> containing information about added, edited, and deleted entities.</param>
        /// <param name="DetailedComparison">Specifies whether to consider detailed property changes for edited entities. Default is true.</param>
        /// <returns>
        /// A <see cref="ChangeType"/> enum value indicating the overall change type:
        /// <list type="bullet">
        ///     <item>
        ///         <description><see cref="ChangeType.All"/> - Added, edited, and deleted entities are present.</description>
        ///     </item>
        ///     <item>
        ///         <description><see cref="ChangeType.Added"/> - Only added entities are present.</description>
        ///     </item>
        ///     <item>
        ///         <description><see cref="ChangeType.AddedWithEdited"/> - Added and edited entities are present (with or without detailed property changes).</description>
        ///     </item>
        ///     <item>
        ///         <description><see cref="ChangeType.Edited"/> - Only edited entities are present (with or without detailed property changes).</description>
        ///     </item>
        ///     <item>
        ///         <description><see cref="ChangeType.EditedWithDeleted"/> - Edited entities and deleted entities are present (with or without detailed property changes).</description>
        ///     </item>
        ///     <item>
        ///         <description><see cref="ChangeType.Deleted"/> - Only deleted entities are present.</description>
        ///     </item>
        ///     <item>
        ///         <description><see cref="ChangeType.None"/> - No significant changes are detected.</description>
        ///     </item>
        /// </list>
        /// </returns>
        public ChangeType DetermineChangeType<T>(Result<T> result, bool DetailedComparison)
        {
            // Tuple representing combinations of hasAdded, hasEdited, and hasDeleted
            (bool, bool, bool) key;
            if (DetailedComparison)
                key = (result.Added.Count > 0, result.EditedDetailed.Count > 0, result.Deleted.Count > 0);
            else
                key = (result.Added.Count > 0, result.Edited.Count > 0, result.Deleted.Count > 0);

            // Switch based on the combinations
            switch (key)
            {
                case (true, true, true):
                    // Added, EditedDetailed, and Deleted present
                    return ChangeType.All;

                case (true, false, false):
                    // Only Added present
                    return ChangeType.Added;

                case (true, true, false):
                    // Added and EditedDetailed present
                    return ChangeType.AddedWithEdited;

                case (false, true, false):
                    // Only EditedDetailed present
                    return ChangeType.Edited;

                case (false, true, true):
                    // EditedDetailed and Deleted present
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

        #region Private Methods

        /// <summary>
        /// Identifies edited entries between two sets of data entities by comparing properties.
        /// </summary>
        /// <param name="Added">A set of data entities added to the destination set.</param>
        /// <param name="Removed">A set of data entities removed from the destination set.</param>
        /// <param name="keyComparer">An equality comparer for identifying key properties.</param>
        /// <param name="CompariablePropertyComparer">An equality comparer for identifying properties used in the comparison.</param>
        /// <returns>
        /// A list of tuples representing edited entries, each containing the source contract and an array of updated properties.
        /// The updated properties include the property name and the new value.
        /// </returns>
        private List<(T sourceContract, (string propName, object propValue)[] editedProperties)> GetEdited<T>(
            HashSet<T> Added,
            HashSet<T> Removed,
            PropertyEqualityComparer<T> keyComparer,
            PropertyEqualityComparer<T> CompariablePropertyComparer)
        {
            List<(T edit, (string propName, object propValue)[] updatedProperties)> edited = new();

            // Identify edited entries
            var sourceKeyDictionary = Added
                .ToDictionary(row => GenerateCompositeKey(row, keyComparer.properties), row => row);

            var destinationKeyDictionary = Removed
                .ToDictionary(row => GenerateCompositeKey(row, keyComparer.properties), row => row);

            foreach (var item in sourceKeyDictionary)
            {
                if (destinationKeyDictionary.TryGetValue(item.Key, out var destinationContract))
                {
                    (string propName, object propValue)[] updatedProperties = CompariablePropertyComparer.properties
                       .Where(prop => !EqualityComparer<object>.Default.Equals(prop.GetValue(item.Value), prop.GetValue(destinationContract)))
                       .Select(prop => (prop.Name, Value: prop.GetValue(item.Value)!))
                       .ToArray();

                    edited.Add((item.Value, updatedProperties));
                }
            }

            return edited;
        }

        /// <summary>
        /// Generates a composite key based on specified properties for an entity of type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="dataContract">The entity for which the composite key is generated.</param>
        /// <param name="keyProperties">The properties used to create the composite key.</param>
        /// <returns>A string representing the composite key.</returns>
        private string GenerateCompositeKey<T>(T dataContract, PropertyInfo[] keyProperties)
        {
            var values = new string[keyProperties.Length];

            for (int i = 0; i < keyProperties.Length; i++)
            {
                values[i] = keyProperties[i].GetValue(dataContract)?.ToString()!;
            }

            return string.Join("_", values);
        }

        #endregion

    }
}
