using DbSyncKit.Core.Fetcher;
using DbSyncKit.Core.SqlBuilder;
using DbSyncKit.DB.Factory;
using Microsoft.Extensions.DependencyInjection;

namespace DbSyncKit.Core.Extensions
{
    /// <summary>
    /// Extension methods for <see cref="IServiceCollection"/> to simplify the registration
    /// of services related to data synchronization.
    /// </summary>
    public static class SynchronizationServiceCollectionExtensions
    {
        /// <summary>
        /// Registers services related to data synchronization as singletons in the specified
        /// <see cref="IServiceCollection"/>.
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection"/> to register services with.</param>
        /// <remarks>
        /// This method registers the following services:
        /// <list type="bullet">
        ///   <item><see cref="DataContractFetcher"/> as a singleton.</item>
        ///   <item><see cref="QueryBuilder"/> as a singleton.</item>
        ///   <item><see cref="Synchronization"/> as a singleton.</item>
        /// </list>
        /// </remarks>
        public static void AddSynchronizationServices(this IServiceCollection services)
        {
            // Register DataContractFetcher, QueryBuilder, QueryGeneratorFactory, and Synchronization
            services.AddSingleton<QueryGeneratorFactory>();
            services.AddScoped<DataContractFetcher>();
            services.AddTransient<QueryBuilder>();
            services.AddScoped<Synchronization>();
        }
    }

}
