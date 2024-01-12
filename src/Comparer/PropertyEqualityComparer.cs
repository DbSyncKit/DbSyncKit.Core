using DbSyncKit.DB.Interface;
using System.Reflection;

namespace DbSyncKit.Core.Comparer
{
    /// <summary>
    /// Compares instances of data contracts based on specified properties, which can be either key or comparable properties.
    /// </summary>
    /// <typeparam name="T">Type of the data contract implementing <see cref="IDataContract"/>.</typeparam>
    public class PropertyEqualityComparer<T> : IEqualityComparer<T> where T : IDataContract
    {
        /// <summary>
        /// Gets the array of <see cref="PropertyInfo"/> objects representing properties used for equality comparison.
        /// These properties can serve as either key or comparable properties.
        /// </summary>
        public readonly PropertyInfo[] properties;

        /// <summary>
        /// Initializes a new instance of the <see cref="PropertyEqualityComparer{T}"/> class.
        /// </summary>
        /// <param name="Properties">An array of <see cref="PropertyInfo"/> objects representing properties used for equality comparison. These can be either key or comparable properties.</param>
        public PropertyEqualityComparer(PropertyInfo[] Properties)
        {
            properties = Properties;
        }

        /// <summary>
        /// Determines whether two instances of the data contract are equal based on the specified properties.
        /// </summary>
        /// <param name="x">The first instance to compare.</param>
        /// <param name="y">The second instance to compare.</param>
        /// <returns><c>true</c> if the instances are equal; otherwise, <c>false</c>.</returns>
        public bool Equals(T? x, T? y)
        {
            return properties.All(prop => Equals(prop.GetValue(x), prop.GetValue(y)));
        }

        /// <summary>
        /// Returns a hash code for the specified instance of the data contract based on the specified properties.
        /// </summary>
        /// <param name="obj">The instance for which to get the hash code.</param>
        /// <returns>A hash code for the specified instance.</returns>
        public int GetHashCode(T obj)
        {
            unchecked
            {
                int hash = 17;

                foreach (var prop in properties)
                {
                    var value = prop.GetValue(obj);
                    hash = hash ^ ((value?.GetHashCode() ?? 0) + 23);
                }

                return hash;
            }
        }
    }
}
