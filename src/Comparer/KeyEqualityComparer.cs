using DbSyncKit.DB.Interface;
using System.Reflection;

namespace DbSyncKit.Core.Comparer
{
    /// <summary>
    /// Compares instances of data contracts based on specified key Properties.
    /// </summary>
    /// <typeparam name="T">Type of the data contract implementing <see cref="IDataContractComparer"/>.</typeparam>
    public class KeyEqualityComparer<T> : IEqualityComparer<T> where T : IDataContractComparer
    {
        /// <summary>
        /// Gets the array of <see cref="PropertyInfo"/> objects representing properties used for data comparison.
        /// </summary>
        public readonly PropertyInfo[] compariableProperties;

        /// <summary>
        /// Gets the array of <see cref="PropertyInfo"/> objects representing key properties used for unique identification of objects.
        /// </summary>
        public readonly PropertyInfo[] keyProperties;

        /// <summary>
        /// Initializes a new instance of the <see cref="KeyEqualityComparer{T}"/> class.
        /// </summary>
        /// <param name="CompariableProperties">An array of <see cref="PropertyInfo"/> objects representing properties used for data comparison.</param>
        /// <param name="KeyProperties">An array of <see cref="PropertyInfo"/> objects representing key properties used for unique identification of objects.</param>
        public KeyEqualityComparer(PropertyInfo[] CompariableProperties, PropertyInfo[] KeyProperties)
        {
            compariableProperties = CompariableProperties;
            keyProperties = KeyProperties;
        }

        /// <summary>
        /// Determines whether two instances of the data contract are equal based on Compariable properties.
        /// </summary>
        /// <param name="x">The first instance to compare.</param>
        /// <param name="y">The second instance to compare.</param>
        /// <returns><c>true</c> if the instances are equal; otherwise, <c>false</c>.</returns>
        public bool Equals(T? x, T? y)
        {
            return compariableProperties.All(prop => Equals(prop.GetValue(x), prop.GetValue(y)));
        }

        /// <summary>
        /// Returns a hash code for the specified instance of the data contract based on key properties.
        /// </summary>
        /// <param name="obj">The instance for which to get the hash code.</param>
        /// <returns>A hash code for the specified instance.</returns>
        public int GetHashCode(T obj)
        {
            unchecked
            {
                int hash = 17;

                foreach (var prop in keyProperties)
                {
                    var value = prop.GetValue(obj);
                    hash = hash ^ ((value?.GetHashCode() ?? 0) + 23);
                }

                return hash;
            }
        }
    }
}
