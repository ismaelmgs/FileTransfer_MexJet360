// Decompiled with JetBrains decompiler
// Type: FileTransfer_MexJet_360.DataAccess.DBBase
// Assembly: FileTransfer_MexJet_360, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: 51C0F2EE-2D8C-4E2B-B102-38D0C4F03E12
// Assembly location: E:\AerolineasEjecutivas\Codigos\FileTransferMexJet360\ejecutable\FileTransfer_MexJet_360.exe

using NucleoBase.BaseDeDatos;
using NucleoBase.Core;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;

namespace FileTransfer_MexJet_360.DataAccess
{
    public class DBBase
    {
        public BD_SP oBD_SP = new BD_SP();

        public DBBase() => this.oBD_SP.sConexionSQL = Globales.GetConfigConnection("SqlALEMexJet");

        public DBBase(int idConexion)
        {
            if (idConexion == 1)
                this.oBD_SP.sConexionSQL = Globales.GetConfigConnection("SqlALEMexJet");
            if (idConexion != 2)
                return;
            this.oBD_SP.sConexionSQL = Globales.GetConfigConnection("SqlSyteLine");
        }

        public static string conn => ConfigurationManager.ConnectionStrings["SqlALEMexJet"].ConnectionString;

        public static string Provider => ConfigurationManager.ConnectionStrings["SqlALEMexJet"].ProviderName;

        public static DbProviderFactory dpf => DbProviderFactories.GetFactory(DBBase.Provider);

        public static int ejecutaNonQuery(string StoredProcedure, List<DbParameter> parametros)
        {
            try
            {
                int num = 0;
                using (DbConnection connection = DBBase.dpf.CreateConnection())
                {
                    connection.ConnectionString = DBBase.conn;
                    using (DbCommand command = DBBase.dpf.CreateCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = StoredProcedure;
                        command.CommandType = CommandType.StoredProcedure;
                        command.CommandTimeout = 0;
                        foreach (DbParameter parametro in parametros)
                            command.Parameters.Add((object)parametro);
                        connection.Open();
                        num = command.ExecuteNonQuery();
                    }
                }
                return num;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}
