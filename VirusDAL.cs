using CoronavirusModels;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace Virus.Data.Layer
{
    public class VirusDAL
    {
        public void SaveCases(IEnumerable<Case> cases)
        {
            SqlConnection connection = new SqlConnection(@"Data Source=.\SQLEXPRESS;Initial Catalog=Josh;Integrated Security=SSPI;");
            connection.Open();

            foreach (var cCase in cases)
            {
                SqlCommand cmd = new SqlCommand();

                cmd.CommandText = @"Insert into Cases Values (@TotalCases, @TotalDeaths, @TotalRecoveries, @DateAdded)";

                cmd.Parameters.AddWithValue("@TotalCases", cCase.TotalCases);
                cmd.Parameters.AddWithValue("@TotalDeaths", cCase.TotalDeaths);
                cmd.Parameters.AddWithValue("@TotalRecoveries", cCase.TotalRecoveries);
                cmd.Parameters.AddWithValue("@DateAdded", cCase.DateAdded);

                cmd.Connection = connection;

                cmd.ExecuteNonQuery();

            }

            connection.Close();
        }

        public void SaveDataCaseBlob(CaseBlobs caseBlobs)
        {
            SqlConnection connection = new SqlConnection(@"Data Source=.\SQLEXPRESS;Initial Catalog=Josh;Integrated Security=SSPI;");
            connection.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = @"Insert into CaseBlobs Values (@casesBlobId, @DateScraped, @TotalCasesBlob, @TotalDeathsBlob, @TotalRecoveriesBlob, @Processed)";

            cmd.Parameters.AddWithValue("@casesBlobId", caseBlobs.casesBlobId);
            cmd.Parameters.AddWithValue("@DateScraped", caseBlobs.DateScraped);
            cmd.Parameters.AddWithValue("@TotalCasesBlob", caseBlobs.TotalCasesBlob);
            cmd.Parameters.AddWithValue("@TotalDeathsBlob", caseBlobs.TotalDeathsBlob);
            cmd.Parameters.AddWithValue("@TotalRecoveriesBlob", caseBlobs.TotalRecoveriesBlob);
            cmd.Parameters.AddWithValue("@Processed", caseBlobs.Processed);

            cmd.Connection = connection;

            cmd.ExecuteNonQuery();

            connection.Close();
        }

        public void SaveErrorLog(VirusErrorLog error)
        {
            SqlConnection connection = new SqlConnection(@"Data Source=.\SQLEXPRESS;Initial Catalog=Josh;Integrated Security=SSPI;");
            connection.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = @"Insert into VirusErrorLog Values (@ExceptionMessage, @DateAdded, @StackTrace, @ApplicationName)";

            cmd.Parameters.AddWithValue("@ExceptionMessage", error.ExceptionMessage);
            cmd.Parameters.AddWithValue("@DateAdded", DateTime.Now);
            cmd.Parameters.AddWithValue("@StackTrace", error.StackTrace);
            cmd.Parameters.AddWithValue("@ApplicationName", error.ApplicationName);

            cmd.Connection = connection;

            cmd.ExecuteNonQuery();
        }

        public List<CaseBlobs> GetUnprocessedCaseBlobs()
        {
            List<CaseBlobs> blobs = new List<CaseBlobs>();

            SqlConnection connection = new SqlConnection(@"Data Source=.\SQLEXPRESS;Initial Catalog=Josh;Integrated Security=SSPI;");
            connection.Open();

            SqlCommand cmd = new SqlCommand();

            cmd.CommandText = @"SELECT [casesBlobId]
                                      ,[DateScraped]
                                      ,[TotalCasesBlob]
                                      ,[TotalDeathsBlob]
                                      ,[TotalRecoveriesBlob]
                                      ,[Processed]
                                  FROM [dbo].[CaseBlobs]
                                  WHERE Processed = 0";

            cmd.Connection = connection;

            SqlDataReader reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                CaseBlobs l = new CaseBlobs();

                l.casesBlobId = reader.GetInt32(0);
                l.DateScraped = reader.GetDateTime(1);
                l.TotalCasesBlob = reader.GetString(2);
                l.TotalDeathsBlob = reader.GetString(3);
                l.TotalRecoveriesBlob = reader.GetString(4);
                l.Processed = reader.GetBoolean(5);


                blobs.Add(l);
            }

            cmd.Connection = connection;

            return blobs;
        }

        public void MarkProcessed(int casesBlobId)
        {
            SqlConnection connection = new SqlConnection(@"Data Source=.\SQLEXPRESS;Initial Catalog=Josh;Integrated Security=SSPI;");
            connection.Open();

            SqlCommand cmd = new SqlCommand();
            cmd.Parameters.AddWithValue("@casesBlobId", casesBlobId);

            cmd.CommandText = @"Update CaseBlobs

                                Set Processed = 1

                                Where casesBlobId = @casesBlobId";

            cmd.Connection = connection;
            cmd.ExecuteNonQuery();
        }
    }
}
