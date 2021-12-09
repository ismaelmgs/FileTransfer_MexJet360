// Decompiled with JetBrains decompiler
// Type: FileTransfer_MexJet_360.DataAccess.DBALEMexJet
// Assembly: FileTransfer_MexJet_360, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: 51C0F2EE-2D8C-4E2B-B102-38D0C4F03E12
// Assembly location: E:\AerolineasEjecutivas\Codigos\FileTransferMexJet360\ejecutable\FileTransfer_MexJet_360.exe

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Reflection;
using System.Text.RegularExpressions;
using NucleoBase.Core;

namespace FileTransfer_MexJet_360.DataAccess
{
  public class DBALEMexJet : DBBase
  {
    public string sFechaLAstEjecucion = "";
    public static string sFechaLastUpdt_Aeropuertos = "";
    public static string sFechaLastUpdt_Pilotos = "";
    public static string sUltimaCargaTripCrew = "";
    public static string sUltimaCargaTripMain = "";
    public static string sUltimaCargaBitacorasPOMAIN = "";
    public static string sUltimaCargaBitacorasPOCREW = "";
    public static string sUltimaCargaBitacorasPOLEGS = "";
    public static string sUltimaCargaTripLegs = "";
    public SqlConnection oscConnection = new SqlConnection();

    public bool TestConnection()
    {
      try
      {
        this.oscConnection.ConnectionString = new DBBase(1).oBD_SP.sConexionSQL;
        this.oscConnection.Open();
        return true;
      }
      catch (SqlException ex)
      {
        throw ex;
      }
    }

    public bool VerificarUltimoEnvioAeropuertos()
    {
      try
      {
        DBALEMexJet.sFechaLastUpdt_Aeropuertos = this.oBD_SP.EjecutarDS_DeQuery("SELECT MAX(CONVERT(DATETIME,LASTUPDT)) FROM [FileTransfer].[tmp_MXJ_Auxiliar_Aeropuertos] WITH(NOLOCK)").Tables[0].Rows[0][0].ToString();
        return !string.IsNullOrEmpty(DBALEMexJet.sFechaLastUpdt_Aeropuertos);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public bool VerificarUltimoEnvioPilotos()
    {
      try
      {
        DBALEMexJet.sFechaLastUpdt_Pilotos = this.oBD_SP.EjecutarDS_DeQuery("SELECT MAX(CONVERT(DATETIME,FechaCreacion)) FROM [FileTransfer].[tmp_MXJ_Auxiliar_Pilotos] WITH(NOLOCK)").Tables[0].Rows[0][0].ToString();
        return !string.IsNullOrEmpty(DBALEMexJet.sFechaLastUpdt_Pilotos);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public bool VerificarUltimoEnvioTripCrew()
    {
      try
      {
        DBALEMexJet.sUltimaCargaTripCrew = this.oBD_SP.EjecutarDS_DeQuery("SELECT MAX(CONVERT(DATETIME,FechaCreacion)) FROM FileTransfer.tmp_MXJ_Auxiliar_TripCrew WITH (NOLOCK)").Tables[0].Rows[0][0].ToString();
        return !string.IsNullOrEmpty(DBALEMexJet.sUltimaCargaTripCrew);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public bool VerificarUltimoEnvioTripMain()
    {
      try
      {
        DBALEMexJet.sUltimaCargaTripMain = this.oBD_SP.EjecutarDS_DeQuery("SELECT MAX(CONVERT(DATETIME,FechaCreacion)) FROM [FileTransfer].[tmp_MXJ_Auxiliar_TripMain] WITH (NOLOCK)").Tables[0].Rows[0][0].ToString();
        return !string.IsNullOrEmpty(DBALEMexJet.sUltimaCargaTripMain);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public bool VerificarUltimoEnvioBitacoras()
    {
      try
      {
        DBALEMexJet.sUltimaCargaBitacorasPOMAIN = this.oBD_SP.EjecutarDS_DeQuery("SELECT MAX(CONVERT(DATETIME,LASTUPDT)) FROM [FileTransfer].[tmp_MXJ_Auxiliar_Bitacoras_POMAIN] WITH(NOLOCK)").Tables[0].Rows[0][0].ToString();
        DBALEMexJet.sUltimaCargaBitacorasPOCREW = this.oBD_SP.EjecutarDS_DeQuery("SELECT MAX(CONVERT(DATETIME,LASTUPDT)) FROM [FileTransfer].[tmp_MXJ_Auxiliar_Bitacoras_POCREW] WITH(NOLOCK)").Tables[0].Rows[0][0].ToString();
        DBALEMexJet.sUltimaCargaBitacorasPOLEGS = this.oBD_SP.EjecutarDS_DeQuery("SELECT MAX(CONVERT(DATETIME,LASTUPDT)) FROM [FileTransfer].[tmp_MXJ_Auxiliar_Bitacoras_POLEGS] WITH(NOLOCK)").Tables[0].Rows[0][0].ToString();
        return !string.IsNullOrEmpty(DBALEMexJet.sUltimaCargaBitacorasPOMAIN);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public bool VerificarUltimoEnvioTripLeg()
    {
      try
      {
        DBALEMexJet.sUltimaCargaTripLegs = this.oBD_SP.EjecutarDS_DeQuery("SELECT MAX(LASTUPDT) FROM [FileTransfer].[tmp_MXJ_Auxiliar_TripLegs] WITH(NOLOCK)").Tables[0].Rows[0][0].ToString();
        return !string.IsNullOrEmpty(DBALEMexJet.sUltimaCargaTripLegs);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public void EjecutaPreValidacionBitacoras()
    {
      try
      {
        this.ejecutarStoredPreValidacionTransferencia();
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public void CopiarRegistrosAeropuertos(DataSet ds)
    {
        try
        {
            this.oscConnection.ConnectionString = this.oBD_SP.sConexionSQL;
            SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(this.oscConnection);
            sqlBulkCopy.DestinationTableName = "[FileTransfer].[tmp_MXJ_Auxiliar_Aeropuertos]";
            this.oscConnection.Open();
            sqlBulkCopy.WriteToServer(ds.Tables[0]);
            this.oscConnection.Close();
            if (string.IsNullOrEmpty(DBALEMexJet.sFechaLastUpdt_Aeropuertos))
                DBALEMexJet.sFechaLastUpdt_Aeropuertos = "01/01/1800";
            DataSet dataSet1 = new DataSet();
            DataSet dataSet2 = this.oBD_SP.EjecutarDS("[FileTransfer].[spS_MXJ_TMP_FileTransfer]", (object) "@Accion", (object) 1, (object) "@FechaLastUpdtAeropuerto", (object) Convert.ToDateTime(DBALEMexJet.sFechaLastUpdt_Aeropuertos));
            sqlBulkCopy.DestinationTableName = "[Catalogos].[tbc_MXJ_Aeropuerto]";
            this.oscConnection.Open();
            sqlBulkCopy.WriteToServer(dataSet2.Tables[0]);
            this.oscConnection.Close();
            this.oBD_SP.EjecutarDS("[FileTransfer].[spD_MXJ_EliminaAeropuertosRepetidos]");
        }
        catch (Exception ex)
        {
            throw ex;
        }
    }

    public void CopiarRegistrosPilotos(DataSet ds)
    {
      try
      {
        this.oscConnection.ConnectionString = this.oBD_SP.sConexionSQL;
        SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(this.oscConnection);
        sqlBulkCopy.DestinationTableName = "[FileTransfer].[tmp_MXJ_Auxiliar_Pilotos]";
        this.oscConnection.Open();
        sqlBulkCopy.WriteToServer(ds.Tables[0]);
        this.oscConnection.Close();
        if (string.IsNullOrEmpty(DBALEMexJet.sFechaLastUpdt_Pilotos))
          DBALEMexJet.sFechaLastUpdt_Pilotos = "01/01/1900";
        DataSet dataSet1 = new DataSet();
        DataSet dataSet2 = this.oBD_SP.EjecutarDS("[FileTransfer].[spS_MXJ_TMP_FileTransfer]", (object) "@Accion", (object) 2, (object) "@FechaLastUpdtPilotos", (object) Convert.ToDateTime(DBALEMexJet.sFechaLastUpdt_Pilotos));
        sqlBulkCopy.DestinationTableName = "[Catalogos].[tbc_MXJ_Pilotos]";
        this.oscConnection.Open();
        sqlBulkCopy.WriteToServer(dataSet2.Tables[0]);
        this.oscConnection.Close();
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public void CopiarRegistrosTripCrew(DataSet ds)
    {
      try
      {
        SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(this.oscConnection);
        DataSet dataSet = new DataSet();
        this.oscConnection.ConnectionString = this.oBD_SP.sConexionSQL;
        sqlBulkCopy.DestinationTableName = "FileTransfer.tmp_MXJ_Auxiliar_TripCrew";
        this.oscConnection.Open();
        sqlBulkCopy.WriteToServer(ds.Tables[0]);
        this.oscConnection.Close();
        this.oBD_SP.EjecutarDS("[FileTransfer].[spS_MXJ_TMP_FileTransfer]", (object) "@Accion", (object) 5);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public void CopiarRegistrosTripLeg(DataSet ds)
    {
      SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(this.oscConnection);
      try
      {
        DataSet dataSet = new DataSet();
        this.oscConnection.ConnectionString = this.oBD_SP.sConexionSQL;
        sqlBulkCopy.DestinationTableName = "FileTransfer.tmp_MXJ_Auxiliar_TripLegs";
        this.oscConnection.Open();
        sqlBulkCopy.BulkCopyTimeout = 0;
        sqlBulkCopy.WriteToServer(ds.Tables[0]);
        this.oscConnection.Close();
        this.ejecutarStoredTransferenciaFightPack();
      }
      catch (Exception ex)
      {
        Console.WriteLine(ex.Message);
        this.oscConnection.Close();
        if (ex.Message.Contains("Received an invalid column length from the bcp client for colid"))
        {
          string pattern = "\\d+";
          int index = Convert.ToInt32(Regex.Match(ex.Message.ToString(), pattern).Value) - 1;
          object obj1 = typeof (SqlBulkCopy).GetField("_sortedColumnMappings", BindingFlags.Instance | BindingFlags.NonPublic).GetValue((object) sqlBulkCopy);
          object[] objArray = (object[]) obj1.GetType().GetField("_items", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(obj1);
          object obj2 = objArray[index].GetType().GetField("_metadata", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(objArray[index]);
          object obj3 = obj2.GetType().GetField("column", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic).GetValue(obj2);
          object obj4 = obj2.GetType().GetField("length", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic).GetValue(obj2);
          Console.WriteLine(string.Format("Column: {0} contains data with a length greater than: {1}", obj3, obj4));
          throw new Exception(string.Format("Column: {0} contains data with a length greater than: {1}", obj3, obj4));
        }
        throw;
      }
    }

    public void ejecutarStoredTransferenciaFightPack()
    {
      try
      {
        List<DbParameter> parametros = new List<DbParameter>();
        DbParameter parameter1 = DBBase.dpf.CreateParameter();
        parameter1.DbType = DbType.Int32;
        parameter1.Value = (object) 7;
        parameter1.ParameterName = "Accion";
        parametros.Add(parameter1);
        DbParameter parameter2 = DBBase.dpf.CreateParameter();
        parameter2.DbType = DbType.Int32;
        parameter2.Value = (object) null;
        parameter2.ParameterName = "idEjecucion";
        parametros.Add(parameter2);
        DbParameter parameter3 = DBBase.dpf.CreateParameter();
        parameter3.DbType = DbType.DateTime;
        parameter3.Value = (object) null;
        parameter3.ParameterName = "FechaLastUpdtAeropuerto";
        parametros.Add(parameter3);
        DbParameter parameter4 = DBBase.dpf.CreateParameter();
        parameter4.DbType = DbType.DateTime;
        parameter4.Value = (object) null;
        parameter4.ParameterName = "FechaLastUpdtPilotos";
        parametros.Add(parameter4);
        DbParameter parameter5 = DBBase.dpf.CreateParameter();
        parameter5.DbType = DbType.DateTime;
        parameter5.Value = (object) null;
        parameter5.ParameterName = "FechaLastUpdtBitacoras";
        parametros.Add(parameter5);
        DateTime dateTime;
        if (string.IsNullOrEmpty(DBALEMexJet.sUltimaCargaTripMain))
        {
          DBALEMexJet.sUltimaCargaTripMain = "01/01/1900";
          dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripMain);
        }
        else if (string.IsNullOrEmpty(DBALEMexJet.sUltimaCargaTripLegs))
        {
          DBALEMexJet.sUltimaCargaTripLegs = "01/01/1900";
          dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripLegs);
        }
        else
          dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripLegs);
        DbParameter parameter6 = DBBase.dpf.CreateParameter();
        parameter6.DbType = DbType.DateTime;
        parameter6.Value = (object) dateTime;
        parameter6.ParameterName = "FechaLAstUpdtSolFlightPack";
        parametros.Add(parameter6);
        DBBase.ejecutaNonQuery("[FileTransfer].[spS_MXJ_TMP_FileTransfer]", parametros);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public void CopiarRegistrosBitacora(DataSet ds)
    {
      if (this.oscConnection.State == ConnectionState.Open)
        this.oscConnection.Close();
      this.oscConnection.ConnectionString = this.oBD_SP.sConexionSQL;
      try
      {
        SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(this.oscConnection);
        sqlBulkCopy.DestinationTableName = "[FileTransfer].[tmp_MXJ_Auxiliar_Bitacoras_POMAIN]";
        this.oscConnection.Open();
        sqlBulkCopy.WriteToServer(ds.Tables["POMAIN"]);
        this.oscConnection.Close();
        sqlBulkCopy.DestinationTableName = "[FileTransfer].[tmp_MXJ_Auxiliar_Bitacoras_POCREW]";
        this.oscConnection.Open();
        sqlBulkCopy.WriteToServer(ds.Tables["POCREW"]);
        this.oscConnection.Close();
        sqlBulkCopy.DestinationTableName = "[FileTransfer].[tmp_MXJ_Auxiliar_Bitacoras_POLEGS]";
        this.oscConnection.Open();
        sqlBulkCopy.WriteToServer(ds.Tables["POLEGS"]);
        this.oscConnection.Close();
        if (string.IsNullOrEmpty(DBALEMexJet.sUltimaCargaBitacorasPOMAIN))
          DBALEMexJet.sUltimaCargaBitacorasPOMAIN = "01/01/1900";
        if (this.oscConnection.State == ConnectionState.Open)
          this.oscConnection.Close();
        this.ejecutarStoredTransferencia();
        this.ejecutarStoredTransferenciaValidacion();
      }
      catch (Exception ex)
      {
        Console.WriteLine(ex.Message);
        this.oscConnection.Close();
      }
    }

    public void ejecutarStoredTransferencia(
      int iAccion,
      string iIdEjecucion,
      string dtFechaLastUpdtAeropuerto,
      string dtFechaLastUpdtPilotos,
      string dtFechaLastUpdtBitacoras,
      string dtFechaLAstUpdtSolFlightPack)
    {
      try
      {
        List<DbParameter> parametros = new List<DbParameter>();
        DbParameter parameter1 = DBBase.dpf.CreateParameter();
        parameter1.DbType = DbType.Int32;
        parameter1.Value = (object) iAccion;
        parameter1.ParameterName = "Accion";
        parametros.Add(parameter1);
        DbParameter parameter2 = DBBase.dpf.CreateParameter();
        parameter2.DbType = DbType.Int32;
        parameter2.Value = (object) iIdEjecucion;
        parameter2.ParameterName = "idEjecucion";
        parametros.Add(parameter2);
        DbParameter parameter3 = DBBase.dpf.CreateParameter();
        parameter3.DbType = DbType.DateTime;
        parameter3.Value = (object) dtFechaLastUpdtAeropuerto;
        parameter3.ParameterName = "FechaLastUpdtAeropuerto";
        parametros.Add(parameter3);
        DbParameter parameter4 = DBBase.dpf.CreateParameter();
        parameter4.DbType = DbType.DateTime;
        parameter4.Value = (object) dtFechaLastUpdtPilotos;
        parameter4.ParameterName = "FechaLastUpdtPilotos";
        parametros.Add(parameter4);
        DbParameter parameter5 = DBBase.dpf.CreateParameter();
        parameter5.DbType = DbType.DateTime;
        parameter5.Value = !string.IsNullOrEmpty(dtFechaLastUpdtBitacoras) ? (object) Convert.ToDateTime(dtFechaLastUpdtBitacoras) : (object) null;
        parameter5.ParameterName = "FechaLastUpdtBitacoras";
        parametros.Add(parameter5);
        DbParameter parameter6 = DBBase.dpf.CreateParameter();
        parameter6.DbType = DbType.String;
        parameter6.Value = (object) dtFechaLAstUpdtSolFlightPack;
        parameter6.ParameterName = "FechaLAstUpdtSolFlightPack";
        parametros.Add(parameter6);
        DBBase.ejecutaNonQuery("[FileTransfer].[spS_MXJ_TMP_FileTransfer]", parametros);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public void ejecutarStoredTransferencia()
    {
      try
      {
        List<DbParameter> parametros = new List<DbParameter>();
        DbParameter parameter1 = DBBase.dpf.CreateParameter();
        parameter1.DbType = DbType.Int32;
        parameter1.Value = (object) 3;
        parameter1.ParameterName = "Accion";
        parametros.Add(parameter1);
        DbParameter parameter2 = DBBase.dpf.CreateParameter();
        parameter2.DbType = DbType.Int32;
        parameter2.Value = (object) null;
        parameter2.ParameterName = "idEjecucion";
        parametros.Add(parameter2);
        DbParameter parameter3 = DBBase.dpf.CreateParameter();
        parameter3.DbType = DbType.DateTime;
        parameter3.Value = (object) null;
        parameter3.ParameterName = "FechaLastUpdtAeropuerto";
        parametros.Add(parameter3);
        DbParameter parameter4 = DBBase.dpf.CreateParameter();
        parameter4.DbType = DbType.DateTime;
        parameter4.Value = (object) null;
        parameter4.ParameterName = "FechaLastUpdtPilotos";
        parametros.Add(parameter4);
        DbParameter parameter5 = DBBase.dpf.CreateParameter();
        parameter5.DbType = DbType.DateTime;
        parameter5.Value = (object) Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOMAIN);
        parameter5.ParameterName = "FechaLastUpdtBitacoras";
        parametros.Add(parameter5);
        DbParameter parameter6 = DBBase.dpf.CreateParameter();
        parameter6.DbType = DbType.String;
        parameter6.Value = (object) null;
        parameter6.ParameterName = "FechaLAstUpdtSolFlightPack";
        parametros.Add(parameter6);
        DBBase.ejecutaNonQuery("[FileTransfer].[spS_MXJ_TMP_FileTransfer]", parametros);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public void ejecutarStoredTransferenciaValidacion()
    {
      try
      {
        List<DbParameter> parametros = new List<DbParameter>();
        DbParameter parameter1 = DBBase.dpf.CreateParameter();
        parameter1.DbType = DbType.Int32;
        parameter1.Value = (object) 4;
        parameter1.ParameterName = "Accion";
        parametros.Add(parameter1);
        DbParameter parameter2 = DBBase.dpf.CreateParameter();
        parameter2.DbType = DbType.Int32;
        parameter2.Value = (object) null;
        parameter2.ParameterName = "idEjecucion";
        parametros.Add(parameter2);
        DbParameter parameter3 = DBBase.dpf.CreateParameter();
        parameter3.DbType = DbType.DateTime;
        parameter3.Value = (object) null;
        parameter3.ParameterName = "FechaLastUpdtAeropuerto";
        parametros.Add(parameter3);
        DbParameter parameter4 = DBBase.dpf.CreateParameter();
        parameter4.DbType = DbType.DateTime;
        parameter4.Value = (object) null;
        parameter4.ParameterName = "FechaLastUpdtPilotos";
        parametros.Add(parameter4);
        DbParameter parameter5 = DBBase.dpf.CreateParameter();
        parameter5.DbType = DbType.DateTime;
        parameter5.Value = (object) null;
        parameter5.ParameterName = "FechaLastUpdtBitacoras";
        parametros.Add(parameter5);
        DbParameter parameter6 = DBBase.dpf.CreateParameter();
        parameter6.DbType = DbType.String;
        parameter6.Value = (object) null;
        parameter6.ParameterName = "FechaLAstUpdtSolFlightPack";
        parametros.Add(parameter6);
        DBBase.ejecutaNonQuery("[FileTransfer].[spS_MXJ_TMP_FileTransfer]", parametros);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public void ejecutarStoredPreValidacionTransferencia()
    {
      try
      {
        List<DbParameter> parametros = new List<DbParameter>();
        DbParameter parameter = DBBase.dpf.CreateParameter();
        parameter.DbType = DbType.Int32;
        parameter.Value = (object) 1;
        parameter.ParameterName = "Accion";
        parametros.Add(parameter);
        DBBase.ejecutaNonQuery("[FileTransfer].[spS_MXJ_TMP_PreValidaTransferenciaBitacora]", parametros);
       }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public void ejecutarStoredTransferencia2()
    {
      SqlConnection connection = new SqlConnection();
      try
      {
        if (connection.State == ConnectionState.Open)
          connection.Close();
        connection.ConnectionString = this.oBD_SP.sConexionSQL;
        connection.Open();
        SqlCommand sqlCommand = new SqlCommand("[FileTransfer].[spS_MXJ_TMP_FileTransfer]", connection);
        sqlCommand.CommandType = CommandType.StoredProcedure;
        sqlCommand.Parameters.AddWithValue("@Accion", (object) 3);
        sqlCommand.Parameters.AddWithValue("@idEjecucion", (object) null);
        sqlCommand.Parameters.AddWithValue("@FechaLastUpdtAeropuerto", (object) null);
        sqlCommand.Parameters.AddWithValue("@FechaLastUpdtPilotos", (object) null);
        sqlCommand.Parameters.AddWithValue("@FechaLastUpdtBitacoras", (object) Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOMAIN));
        sqlCommand.Parameters.AddWithValue("@FechaLAstUpdtSolFlightPack", (object) null);
        sqlCommand.CommandTimeout = 0;
        sqlCommand.ExecuteNonQuery();
      }
      catch (Exception ex)
      {
        throw ex;
      }
      finally
      {
        if (connection != null)
        {
          connection.Close();
          connection.Dispose();
        }
      }
    }

    public void CopiarRegistrosTripMain(DataSet ds)
    {
      try
      {
        SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(this.oscConnection);
        DataSet dataSet = new DataSet();
        this.oscConnection.ConnectionString = this.oBD_SP.sConexionSQL;
        sqlBulkCopy.DestinationTableName = "FileTransfer.tmp_MXJ_Auxiliar_TripMain";
        this.oscConnection.Open();
        sqlBulkCopy.BulkCopyTimeout = 0;
        sqlBulkCopy.WriteToServer(ds.Tables[0]);
        this.oscConnection.Close();
        if (string.IsNullOrEmpty(DBALEMexJet.sUltimaCargaTripMain))
          DBALEMexJet.sUltimaCargaTripMain = "01/01/1900";
        dataSet = this.oBD_SP.EjecutarDS("[FileTransfer].[spS_MXJ_TMP_FileTransfer]", (object) "@Accion", (object) 6, (object) "@FechaLastUpdtBitacoras", (object) Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripMain));
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }


        public string ObtieneParametroPorClave(string sClave)
        {
            try
            {
                return oBD_SP.EjecutarValor("", "", sClave).S();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
  }
}
