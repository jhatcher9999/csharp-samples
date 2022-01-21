using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Configuration;

using Npgsql;
using Dapper;

namespace csharp_samples
{
    public class SimpleDataService
    {

        private const int MAX_RETRY_COUNT = 3;
        private const int WAIT_TIME_IN_SEC_FOR_CONN_RETRY = 5;
        private const string SQL_STATE_RETRY = "40001";
        private const string SQL_STATE_SERVER_SHUTDOWN = "57P01";
        
        private string _connString;
        private int _id = 1;

        private string _oldName = "john doe"; //23 chars
        private string _newName = "jane doe!";
        private string activeDatabaseConnectionKey = "CockroachK8sAzure";

        private System.Random _rand = new System.Random(); 

        private readonly IConfigurationRoot _config;

        public SimpleDataService(IConfigurationRoot config)
        {

            _config = config;

            //postgresql://jimhatcher:PASSWORDHERE@gp-porticopoc-1-g84.gcp-us-east1.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full&sslrootcert=$HOME/Library/CockroachCloud/certs/gp-porticopoc-1-ca.crt

            IConfigurationSection configSection = config.GetSection("DBConnections").GetSection(activeDatabaseConnectionKey);

            string host = configSection.GetValue<string>("host");
            int port = Int32.Parse(configSection.GetValue<string>("port"));
            string username = configSection.GetValue<string>("username");
            string password = configSection.GetValue<string>("password");
            string database = configSection.GetValue<string>("database");
            string rootCertPath = configSection.GetValue<string>("rootCertPath");

            var connStringBuilder = new NpgsqlConnectionStringBuilder();
            connStringBuilder.Host = host;
            connStringBuilder.Port = port;
            connStringBuilder.SslMode = SslMode.Require; //instead of verify-full, do Require here and TrustServerCertificate = true
            connStringBuilder.Username = username;
            connStringBuilder.Password = password;
            connStringBuilder.Database = database;
            connStringBuilder.RootCertificate = rootCertPath;
            connStringBuilder.TrustServerCertificate = true;

            //connStringBuilder.Multiplexing = true;

            //connection pool settings
            connStringBuilder.Pooling = true;
            connStringBuilder.MinPoolSize = 3;
            connStringBuilder.MaxPoolSize = 3;

            //connStringBuilder.KeepAlive = 150000;
            
            _connString = connStringBuilder.ConnectionString;

        }

        public async Task RunSelectInALoop()
        {

            try {

                var sqlSelect = "SELECT customer_id, name FROM customer LIMIT 5";

                for(int i = 0; i < 100; i++) {

                    Console.WriteLine("Instance " + i.ToString());

                    // Retrieve some data
                    string output = await runSqlAsync(sqlSelect);
                    Console.WriteLine(output);
                    Thread.Sleep(1000);
                    Console.WriteLine();

                }

            }
            catch(Exception ex) {
                Console.WriteLine(ex.ToString());
            }
            finally {
                Console.WriteLine("Exiting RunSelectInLoop() Method");
            }

        }

        private async Task<string> runSqlAsync(String sqlCode, params string[] args)
        {
            StringBuilder sb = new StringBuilder();

            try
            {
                await using (var conn = new NpgsqlConnection(_connString))
                {

                    int executionAttemptCounter = 0;

                    while (executionAttemptCounter <= MAX_RETRY_COUNT)
                    {
                        if (executionAttemptCounter == MAX_RETRY_COUNT)
                        {
                            string err = String.Format("hit max of %s retries, aborting", MAX_RETRY_COUNT);
                            throw new Exception(err);
                        }

                        NpgsqlTransaction trans = null;
                        try
                        {

                            //TODO: add logic to apply parameters to sqlCode
                            //handle SELECT vs UPDATE

                            await conn.OpenAsync();

                            trans = await conn.BeginTransactionAsync();

                            await using (var cmdSelect = new NpgsqlCommand(sqlCode, conn))
                            {
                                await using (var reader = await cmdSelect.ExecuteReaderAsync())
                                {
                                    while (await reader.ReadAsync()) {
                                        sb.Append("customer_id: " + reader.GetInt32(0));
                                        sb.Append("; ");
                                        sb.Append("name: " + reader.GetString(1));
                                        sb.Append("\n");
                                    }
                                }
                            }

                            await trans.CommitAsync();
                            break;
                        }
                        catch(Npgsql.PostgresException pex) when (pex.SqlState == SQL_STATE_SERVER_SHUTDOWN)
                        {

                            executionAttemptCounter++;

                            sb.Append("Hit " + SQL_STATE_SERVER_SHUTDOWN + " server shutdown error\n");
                            sb.Append("This was attempt: " + executionAttemptCounter + "\n");

                            sb.Append("Sleeping for " + WAIT_TIME_IN_SEC_FOR_CONN_RETRY.ToString() + " seconds and trying again\n");
                            Thread.Sleep(WAIT_TIME_IN_SEC_FOR_CONN_RETRY * 1000);
                        }
                        catch(Npgsql.PostgresException pex) when (pex.SqlState == SQL_STATE_RETRY)
                        {
                            if (trans != null)
                            {
                                await trans.RollbackAsync();
                            }
                            executionAttemptCounter++;

                            //apply an exponential backoff (i.e., wait a little longer every time through the loop)
                            int sleepMillis = (int)(Math.Pow(2, executionAttemptCounter) * 100) + _rand.Next(100);
                            sb.Append(String.Format("Hit " + SQL_STATE_RETRY + " transaction retry error, sleeping %s milliseconds\n", sleepMillis));
                            Thread.Sleep(sleepMillis);
                        }
                        catch(Exception ex)
                        {
                            sb.Append(ex.ToString() + "\n");
                            break;
                        }

                        await conn.CloseAsync();

                    }
                }
            }
            catch(Exception ex)
            {
                sb.Append(ex.ToString());
            }

            return sb.ToString();

        }

        public void RunWithNpgsql()
        {

            using (var conn = new NpgsqlConnection(_connString)) {
                conn.Open();


                foreach(int id in new int[]{ 1, 2, 3}) {

                    // Update some data
                    var name = _oldName + " " + _newName + " using NPGSQL";
                    var sqlUpdate = "UPDATE customer SET name = @name WHERE txnid = @customer_id;";
                    using (var cmdUpdate = new NpgsqlCommand(sqlUpdate, conn))
                    {
                        cmdUpdate.Parameters.AddWithValue("customer_id", id);
                        cmdUpdate.Parameters.AddWithValue("name", name);
                        cmdUpdate.ExecuteNonQuery();
                    }

                    // Retrieve some data
                    var sqlSelect = "SELECT customer_id, name FROM custlmer WHERE customer_id = @id;";
                    using (var cmdSelect = new NpgsqlCommand(sqlSelect, conn))
                    {
                        cmdSelect.Parameters.AddWithValue("customer_id", id);
                        using (var reader = cmdSelect.ExecuteReader())
                        {
                            while (reader.Read()) {
                                Console.WriteLine("id: " + reader.GetInt32(0));
                                Console.WriteLine("name: " + reader.GetString(1));
                            }
                        }
                    }

                }

            }

        }

        public void RunWithDapper()
        {

            //Dapper needs an IDBConnection and NpgsqlConnection implements the IDBConnection interface 
            using (IDbConnection conn = new NpgsqlConnection(_connString))
            {
                // Update some data
                Customer c = new Customer();
                c.CustomerID = _id;
                c.Name = _oldName + " " + _newName + " using Dapper";
                var sqlUpdate = "UPDATE customer SET name = @name WHERE customer_id = @id;";
                conn.Execute(sqlUpdate, c);


                // Read some data
                var sqlSelect = "SELECT customer_id, name FROM customer WHERE txnid = @id;";
                var parameters = new { id = _id };
                c = conn.Query<Customer>(sqlSelect, parameters).FirstOrDefault();
                if (c != null) {
                    Console.WriteLine("id: " + c.CustomerID);
                    Console.WriteLine("name: " + c.Name);
                }

            }
            
        }

    }
}
