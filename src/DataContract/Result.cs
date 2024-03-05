using DbSyncKit.Core.Enum;
using DbSyncKit.DB.Interface;

namespace DbSyncKit.Core.DataContract
{
    /// <summary>
    /// Represents the result of a synchronization operation for a specific data type.
    /// </summary>
    /// <typeparam name="T">The type of data being synchronized.</typeparam>
    public class Result<T>
    {
        /// <summary>
        /// Gets or sets the list of entities that were added during synchronization.
        /// </summary>
        /// <remarks>
        /// Entities in this list are present in the source but not in the destination.
        /// </remarks>
        public List<T> Added { get; set; } = new();

        /// <summary>
        /// Gets or sets the list of entities that were deleted during synchronization.
        /// </summary>
        /// <remarks>
        /// Entities in this list are present in the destination but not in the source.
        /// </remarks>
        public List<T> Deleted { get; set; } = new();

        /// <summary>
        /// Gets or sets the list of entities that were edited during synchronization,
        /// along with an array of updated properties for each edited entity.
        /// </summary>
        /// <remarks>
        /// The <see cref="EditedDetailed"/> property contains a list of tuples where:
        /// <list type="bullet">
        ///     <item>
        ///         <description><c>sourceContract</c> is the original entity before synchronization.</description>
        ///     </item>
        ///     <item>
        ///         <description><c>editedProperties</c> is an array of updated properties for the edited entity.</description>
        ///     </item>
        /// </list>
        /// </remarks>
        public List<(T sourceContract, (string propName, object propValue)[] editedProperties)> EditedDetailed { get; set; } = new();


        /// <summary>
        /// Gets or sets the list of entities that were edited during synchronization.
        /// </summary>
        /// <remarks>
        /// The <see cref="Edited"/> property contains a list of entities that were edited during synchronization.
        /// Unlike <see cref="EditedDetailed"/>, it does not provide specific information about updated properties.
        /// </remarks>

        public List<T> Edited { get; set; } = new();



        /// <summary>
        /// Gets or sets the count of data records in the source database.
        /// </summary>
        public long SourceDataCount { get; set; }

        /// <summary>
        /// Gets or sets the count of data records in the destination database.
        /// </summary>
        public long DestinaionDataCount { get; set; }

        /// <summary>
        /// Gets or sets the type of change represented by the synchronization result.
        /// </summary>
        public ChangeType ResultChangeType { get; set; }
    }
}
