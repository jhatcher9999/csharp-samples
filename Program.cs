using System;
using System.Threading.Tasks;

using Microsoft.Extensions.Configuration;

using System.IO;

namespace csharp_samples
{

    class Program
    {
        public static IConfigurationRoot _configuration;

        public static async Task Main(string[] args)
        {

            // Build configuration
            _configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetParent(AppContext.BaseDirectory).FullName)
                .AddJsonFile("appsettings.json", false, true)
                .Build();

            //launch data service
            SimpleDataService sds = new SimpleDataService(_configuration);

            Console.WriteLine("Running read in loop with Npgsql");
            await sds.RunSelectInALoop();
            Console.WriteLine();

            // Console.WriteLine("Running read & write with Npgsql");
            // sds.RunWithNpgsql();
            // Console.WriteLine();

            // Console.WriteLine("Running read & write with Dapper");
            // sds.RunWithDapper();
            // Console.WriteLine();

            Environment.Exit(0);
        
        }
    
    }
}
