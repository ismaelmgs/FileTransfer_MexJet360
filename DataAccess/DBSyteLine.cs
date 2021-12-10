// Decompiled with JetBrains decompiler
// Type: FileTransfer_MexJet_360.DataAccess.DBSyteLine
// Assembly: FileTransfer_MexJet_360, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: 51C0F2EE-2D8C-4E2B-B102-38D0C4F03E12
// Assembly location: E:\AerolineasEjecutivas\Codigos\FileTransferMexJet360\ejecutable\FileTransfer_MexJet_360.exe

using FileTransfer_MexJet_360.Clases;
using System;
using System.Data;
using System.Data.SqlClient;

namespace FileTransfer_MexJet_360.DataAccess
{
    public class DBSyteLine : DBBase
    {
        public string sFechaLAstEjecucion = "";
        public static string sFechaLastUpdt_Aeropuertos = "";
        public static string sFechaLastUpdt_Pilotos = "";
        public static string sFechaLastUpdt_Bitacoras = "";
        public SqlConnection oscConnection = new SqlConnection();

        public bool TestConnection()
        {
            try
            {
                MyGlobals.StepLog = "TestConnection de DBSyteLine";
                this.oscConnection.ConnectionString = new DBBase(2).oBD_SP.sConexionSQL;
                this.oscConnection.Open();
                return true;
            }
            catch (SqlException ex)
            {
                Console.WriteLine("* {0}                                                                         *", (object)ex.Message.ToString());
                return false;
            }
        }

        public string GetMatricula(string AER)
        {
            DataSet dataSet = this.oBD_SP.EjecutarDS_DeQuery("select top 1 AeronaveSerie from Aeronave where AeronaveMatricula=" + AER);
            return !(dataSet.Tables[0].Rows[0][0].ToString() != "") || !(dataSet.Tables[0].Rows[0][0].ToString() != "") ? "" : dataSet.Tables[0].Rows[0][0].ToString();
        }
    }
}
