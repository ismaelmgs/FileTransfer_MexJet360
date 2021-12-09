// Decompiled with JetBrains decompiler
// Type: FileTransfer_MexJet_360.DataAccess.DBFlightPak
// Assembly: FileTransfer_MexJet_360, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: 51C0F2EE-2D8C-4E2B-B102-38D0C4F03E12
// Assembly location: E:\AerolineasEjecutivas\Codigos\FileTransferMexJet360\ejecutable\FileTransfer_MexJet_360.exe

using FileTransfer_MexJet_360.Clases;
using System;
using System.Configuration;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;

namespace FileTransfer_MexJet_360.DataAccess
{
    public class DBFlightPak : DBBase
    {
        private DBALEMexJet oDBALE = new DBALEMexJet();
        //private OleDbConnection oConnection = new OleDbConnection();
        //public static string CadenaOleDbConnection;
        public SqlConnection oscConnection = new SqlConnection();

        public bool TestConnection()
        {
            try
            {
                this.oscConnection.ConnectionString = new DBBaseFPK().oBD_SP.sConexionSQL;
                this.oscConnection.Open();
                return true;
                //DBFlightPak.CadenaOleDbConnection = "Provider = VFPOLEDB.1;Data Source= " + ConfigurationManager.AppSettings["BASEDATOS"].ToString();
                //this.oConnection.ConnectionString = DBFlightPak.CadenaOleDbConnection;
                //this.oConnection.Open();
                //return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

    //public bool RespaldarBaseDatos()
    //{
    //  try
    //  {
    //    string path = ConfigurationManager.AppSettings["NuevaRutaBDF"].ToString();
    //    FileInfo[] files = new DirectoryInfo(ConfigurationManager.AppSettings["RutaBDF"].ToString()).GetFiles();
    //    if (!Directory.Exists(path))
    //      Directory.CreateDirectory(path);
    //    foreach (FileInfo fileInfo in files)
    //      fileInfo.CopyTo(path + "/" + fileInfo.Name, true);
    //    return true;
    //  }
    //  catch (Exception ex)
    //  {
    //    throw ex;
    //  }
    //}

    public void CargarPreValidacionBitacoras()
    {
      try
      {
        this.oDBALE.EjecutaPreValidacionBitacoras();
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public void CargarRegistrosAeropuerto()
    {
      try
      {
        Utils.GuardarBitacora("Obtiene listado de Airopuertos");
        DataSet dataSet = new DataSet();
        DataSet registrosAirport = this.getRegistrosAirport();        
        Console.WriteLine(string.Format("Se cargaran un total de {0} registros.", (object) registrosAirport.Tables[0].Rows.Count));
        Utils.GuardarBitacora(string.Format("Se cargaran un total de {0} registros.", (object)registrosAirport.Tables[0].Rows.Count));

        this.oDBALE.CopiarRegistrosAeropuertos(registrosAirport);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public void CargarRegistrosPiloto()
    {
      try
      {
        DataSet registrosPilotos = this.getRegistrosPilotos();
        Console.WriteLine(string.Format("Se cargaran un total de {0} registros.", (object) registrosPilotos.Tables[0].Rows.Count));
        this.oDBALE.CopiarRegistrosPilotos(registrosPilotos);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public void CargarRegistrosTripCrew()
    {
      try
      {
        DataSet registrosTripCrew = this.getRegistrosTripCrew();
        Console.WriteLine(string.Format("Se cargaran un total de {0} registros.", (object) registrosTripCrew.Tables[0].Rows.Count));
        this.oDBALE.CopiarRegistrosTripCrew(registrosTripCrew);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public void CargarRegistrosTripMain()
    {
      try
      {
        DataSet registrosTripMain = this.getRegistrosTripMain();
        Console.WriteLine(string.Format("Se cargaran un total de {0} registros.", (object) registrosTripMain.Tables[0].Rows.Count));
        this.oDBALE.CopiarRegistrosTripMain(registrosTripMain);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public void CargarRegistrosTripLeg()
    {
      try
      {
        DataSet registrosTripLeg = this.getRegistrosTripLeg();
        Console.WriteLine(string.Format("Se cargaran un total de {0} registros.", (object) registrosTripLeg.Tables[0].Rows.Count));
        this.oDBALE.CopiarRegistrosTripLeg(registrosTripLeg);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public void CargarRegistrosBitacora()
    {
      try
      {
        DataSet registrosBitacoras = this.getRegistrosBitacoras();
        Console.WriteLine(string.Format("Se cargaran un total de {0} registros.", (object) registrosBitacoras.Tables[0].Rows.Count));
        this.oDBALE.CopiarRegistrosBitacora(registrosBitacoras);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public DataSet getRegistrosAirport()
    {
      DataSet dataSet = new DataSet();
      try
      {
        string cmdText;
        if (string.IsNullOrEmpty(DBALEMexJet.sFechaLastUpdt_Aeropuertos))
        {
          cmdText = " SELECT iata,icao_id,nvl(AIRPORT_NM,''''),city,State,CtryDesc,Country,inactive,lastUser,lastupdt FROM Airport WHERE inactive<>''I'' order by lastupdt desc";
        }
        else
        {
          string[] strArray1 = new string[13];
          strArray1[0] = " Select iata,icao_id,nvl(AIRPORT_NM,''''),city,State,CtryDesc,Country,inactive,lastUser,lastupdt FROM Airport  WHERE inactive<>''I'' AND LASTUPDT > DATETIME(";
          string[] strArray2 = strArray1;
          DateTime dateTime = Convert.ToDateTime(DBALEMexJet.sFechaLastUpdt_Aeropuertos);
          int num = dateTime.Year;
          string str1 = num.ToString();
          strArray2[1] = str1;
          strArray1[2] = ", ";
          string[] strArray3 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sFechaLastUpdt_Aeropuertos);
          num = dateTime.Month;
          string str2 = num.ToString();
          strArray3[3] = str2;
          strArray1[4] = ", ";
          string[] strArray4 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sFechaLastUpdt_Aeropuertos);
          num = dateTime.Day;
          string str3 = num.ToString();
          strArray4[5] = str3;
          strArray1[6] = ", ";
          string[] strArray5 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sFechaLastUpdt_Aeropuertos);
          num = dateTime.Hour;
          string str4 = num.ToString();
          strArray5[7] = str4;
          strArray1[8] = ", ";
          string[] strArray6 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sFechaLastUpdt_Aeropuertos);
          num = dateTime.Minute;
          string str5 = num.ToString();
          strArray6[9] = str5;
          strArray1[10] = ", ";
          string[] strArray7 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sFechaLastUpdt_Aeropuertos);
          num = dateTime.Second;
          string str6 = num.ToString();
          strArray7[11] = str6;
          strArray1[12] = ")ORDER BY LASTUPDT DESC";
          cmdText = string.Concat(strArray1);
        }

                string sQuery = "SELECT * FROM OPENQUERY(" + MyGlobals.LinkedServer +", '" + cmdText + "')";

                dataSet = new DBBaseFPK().oBD_SP.EjecutarDS_DeQuery(sQuery);
                //new OleDbDataAdapter(new OleDbCommand(cmdText, this.oConnection)).Fill(dataSet);
                return dataSet;
      }
        catch (Exception ex)
        {
            throw ex;
        }
    }

    public DataSet getRegistrosPilotos()
    {
      DataSet dataSet = new DataSet();
      try
      {
        string cmdText;
        if (string.IsNullOrEmpty(DBALEMexJet.sFechaLastUpdt_Pilotos))
        {
          cmdText = " SELECT CREWCODE, LAST_NAME, FIRST_NAME, nvl(middleinit,'''') ,LASTUSER, LASTUPDT from Crew  order by lastupdt desc";
        }
        else
        {
          string[] strArray1 = new string[13];
          strArray1[0] = " SELECT CREWCODE, LAST_NAME, FIRST_NAME, nvl(middleinit,'''') ,LASTUSER, LASTUPDT from Crew  WHERE LASTUPDT > DATETIME(";
          string[] strArray2 = strArray1;
          DateTime dateTime = Convert.ToDateTime(DBALEMexJet.sFechaLastUpdt_Pilotos);
          int num = dateTime.Year;
          string str1 = num.ToString();
          strArray2[1] = str1;
          strArray1[2] = ", ";
          string[] strArray3 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sFechaLastUpdt_Pilotos);
          num = dateTime.Month;
          string str2 = num.ToString();
          strArray3[3] = str2;
          strArray1[4] = ", ";
          string[] strArray4 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sFechaLastUpdt_Pilotos);
          num = dateTime.Day;
          string str3 = num.ToString();
          strArray4[5] = str3;
          strArray1[6] = ", ";
          string[] strArray5 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sFechaLastUpdt_Pilotos);
          num = dateTime.Hour;
          string str4 = num.ToString();
          strArray5[7] = str4;
          strArray1[8] = ", ";
          string[] strArray6 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sFechaLastUpdt_Pilotos);
          num = dateTime.Minute;
          string str5 = num.ToString();
          strArray6[9] = str5;
          strArray1[10] = ", ";
          string[] strArray7 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sFechaLastUpdt_Pilotos);
          num = dateTime.Second;
          string str6 = num.ToString();
          strArray7[11] = str6;
          strArray1[12] = ")ORDER BY LASTUPDT DESC";
          cmdText = string.Concat(strArray1);
        }

        string sQuery = "SELECT * FROM OPENQUERY(" + MyGlobals.LinkedServer + ", '" + cmdText + "')";               

        dataSet = new DBBaseFPK().oBD_SP.EjecutarDS_DeQuery(sQuery);

        return dataSet;
        //new OleDbDataAdapter(new OleDbCommand(cmdText, this.oConnection)).Fill(dataSet);


      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public DataSet getRegistrosTripCrew()
    {
      DataSet dataSet = new DataSet();
      try
      {
        string cmdText;
        if (string.IsNullOrEmpty(DBALEMexJet.sUltimaCargaTripCrew))
        {
          cmdText = "SELECT ORIG_NMBR, LEGID, LASTUSER, LASTUPDT FROM TRIPCREW ORDER BY LASTUPDT DESC";
        }
        else
        {
          string[] strArray1 = new string[13];
          strArray1[0] = "SELECT ORIG_NMBR, LEGID, LASTUSER, LASTUPDT FROM TRIPCREW  WHERE LASTUPDT > DATETIME(";
          string[] strArray2 = strArray1;
          DateTime dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripCrew);
          int num = dateTime.Year;
          string str1 = num.ToString();
          strArray2[1] = str1;
          strArray1[2] = ", ";
          string[] strArray3 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripCrew);
          num = dateTime.Month;
          string str2 = num.ToString();
          strArray3[3] = str2;
          strArray1[4] = ", ";
          string[] strArray4 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripCrew);
          num = dateTime.Day;
          string str3 = num.ToString();
          strArray4[5] = str3;
          strArray1[6] = ", ";
          string[] strArray5 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripCrew);
          num = dateTime.Hour;
          string str4 = num.ToString();
          strArray5[7] = str4;
          strArray1[8] = ", ";
          string[] strArray6 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripCrew);
          num = dateTime.Minute;
          string str5 = num.ToString();
          strArray6[9] = str5;
          strArray1[10] = ", ";
          string[] strArray7 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripCrew);
          num = dateTime.Second;
          string str6 = num.ToString();
          strArray7[11] = str6;
          strArray1[12] = ")ORDER BY LASTUPDT DESC";
          cmdText = string.Concat(strArray1);
        }

         string sQuery = "SELECT * FROM OPENQUERY(" + MyGlobals.LinkedServer + ", '" + cmdText + "')";
         dataSet = new DBBaseFPK().oBD_SP.EjecutarDS_DeQuery(sQuery);
         return dataSet;
         //new OleDbDataAdapter(new OleDbCommand(cmdText, this.oConnection)).Fill(dataSet);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public DataSet getRegistrosTripMain()
    {
      DataSet dataSet = new DataSet();
      try
      {
        string cmdText;
        if (string.IsNullOrEmpty(DBALEMexJet.sUltimaCargaTripMain))
        {
          cmdText = "SELECT ORIG_NMBR, DESC, TRIP_STAT,  NOTES, TSNOTES, LASTUSER, LASTUPDT, tail_nmbr, LOGFLAG  FROM TRIPMAIN ORDER BY LASTUPDT DESC";
        }
        else
        {
          string[] strArray1 = new string[13];
          strArray1[0] = "SELECT ORIG_NMBR, DESC, TRIP_STAT,  NOTES, TSNOTES, LASTUSER, LASTUPDT, tail_nmbr, LOGFLAG FROM TRIPMAIN  WHERE LASTUPDT > DATETIME(";
          string[] strArray2 = strArray1;
          DateTime dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripMain);
          int num = dateTime.Year;
          string str1 = num.ToString();
          strArray2[1] = str1;
          strArray1[2] = ", ";
          string[] strArray3 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripMain);
          num = dateTime.Month;
          string str2 = num.ToString();
          strArray3[3] = str2;
          strArray1[4] = ", ";
          string[] strArray4 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripMain);
          num = dateTime.Day;
          string str3 = num.ToString();
          strArray4[5] = str3;
          strArray1[6] = ", ";
          string[] strArray5 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripMain);
          num = dateTime.Hour;
          string str4 = num.ToString();
          strArray5[7] = str4;
          strArray1[8] = ", ";
          string[] strArray6 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripMain);
          num = dateTime.Minute;
          string str5 = num.ToString();
          strArray6[9] = str5;
          strArray1[10] = ", ";
          string[] strArray7 = strArray1;
          dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripMain);
          num = dateTime.Second;
          string str6 = num.ToString();
          strArray7[11] = str6;
          strArray1[12] = ")ORDER BY LASTUPDT DESC";
          cmdText = string.Concat(strArray1);
        }
                string sQuery = "SELECT * FROM OPENQUERY(" + MyGlobals.LinkedServer + ", '" + cmdText + "')";
                dataSet = new DBBaseFPK().oBD_SP.EjecutarDS_DeQuery(sQuery);
                return dataSet;
                //new OleDbDataAdapter(new OleDbCommand(cmdText, this.oConnection)).Fill(dataSet);
      }
      catch (Exception ex)
      {
        throw ex;
      }
    }

    public DataSet getRegistrosTripLeg()
    {
        DataSet dataSet = new DataSet();
        try
        {
            string cmdText;
            if (string.IsNullOrEmpty(DBALEMexJet.sUltimaCargaTripLegs))
            {
                cmdText = "SELECT orig_nmbr,leg_num,depicao_id,arricao_id,gmtdep,gmtarr,homdep,homarr,purpose,cat_code,pax_total,client,legid,pic,sic,fltno,LASTUSER, LASTUPDT FROM TRIPLEG ORDER BY LASTUPDT DESC ";
            }
            else
            {
                string[] strArray1 = new string[13];
                strArray1[0] = "SELECT orig_nmbr,leg_num,depicao_id,arricao_id,gmtdep,gmtarr,homdep,homarr,purpose,cat_code,pax_total,client,legid,pic,sic,fltno,LASTUSER, LASTUPDT FROM TRIPLEG  WHERE LASTUPDT > DATETIME(";
                string[] strArray2 = strArray1;
                DateTime dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripLegs);
                int num = dateTime.Year;
                string str1 = num.ToString();
                strArray2[1] = str1;
                strArray1[2] = ", ";
                string[] strArray3 = strArray1;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripLegs);
                num = dateTime.Month;
                string str2 = num.ToString();
                strArray3[3] = str2;
                strArray1[4] = ", ";
                string[] strArray4 = strArray1;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripLegs);
                num = dateTime.Day;
                string str3 = num.ToString();
                strArray4[5] = str3;
                strArray1[6] = ", ";
                string[] strArray5 = strArray1;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripLegs);
                num = dateTime.Hour;
                string str4 = num.ToString();
                strArray5[7] = str4;
                strArray1[8] = ", ";
                string[] strArray6 = strArray1;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripLegs);
                num = dateTime.Minute;
                string str5 = num.ToString();
                strArray6[9] = str5;
                strArray1[10] = ", ";
                string[] strArray7 = strArray1;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaTripLegs);
                num = dateTime.Second;
                string str6 = num.ToString();
                strArray7[11] = str6;
                strArray1[12] = ")ORDER BY LASTUPDT DESC";
                cmdText = string.Concat(strArray1);
            }

                string sQuery = "SELECT * FROM OPENQUERY(" + MyGlobals.LinkedServer + ", '" + cmdText + "')";
                dataSet = new DBBaseFPK().oBD_SP.EjecutarDS_DeQuery(sQuery);
                return dataSet;

                //new OleDbDataAdapter(new OleDbCommand(cmdText, this.oConnection)).Fill(dataSet);
        }
        catch (Exception ex)
        {
            throw ex;
        }
    }

        public DataSet getRegistrosBitacoras()
        {
            DataSet dataSet = new DataSet();
            try
            {
            string cmdText1;
            string cmdText2;
            string cmdText3;
            if (string.IsNullOrEmpty(DBALEMexJet.sUltimaCargaBitacorasPOMAIN))
            {
                cmdText1 = " SELECT TAIL_NMBR,LOGNUM,ORIG_NMBR,TECHLOG, COMPLETED ,LASTUSER,LASTUPDT FROM pomain";
                cmdText2 = " SELECT LOGNUM, CREWCODE, DUTYTYPE, LEGID, LASTUSER, LASTUPDT FROM pocrew";
                cmdText3 = " SELECT TECHLOG, DEPICAO_ID, ARRICAO_ID, CLIENT, SCHEDTTM, TIMEOFF, BLKOUT, FUEL_OUT, PAX_TOTAL, CAT_CODE, TIMEON, BLKIN, FUEL_IN, LEG_NUM, LEGID, LASTUPDT, LOGNUM, INDTTM, OUTDTTM   FROM POLEGS";
            }
            else
            {
                string[] strArray1 = new string[13];
                strArray1[0] = " SELECT TAIL_NMBR,LOGNUM,ORIG_NMBR,TECHLOG, COMPLETED ,LASTUSER,LASTUPDT FROM pomain WHERE LASTUPDT > DATETIME(";
                string[] strArray2 = strArray1;
                DateTime dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOMAIN);
                string str1 = dateTime.Year.ToString();
                strArray2[1] = str1;
                strArray1[2] = ", ";
                string[] strArray3 = strArray1;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOMAIN);
                string str2 = dateTime.Month.ToString();
                strArray3[3] = str2;
                strArray1[4] = ", ";
                string[] strArray4 = strArray1;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOMAIN);
                string str3 = dateTime.Day.ToString();
                strArray4[5] = str3;
                strArray1[6] = ", ";
                string[] strArray5 = strArray1;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOMAIN);
                string str4 = dateTime.Hour.ToString();
                strArray5[7] = str4;
                strArray1[8] = ", ";
                string[] strArray6 = strArray1;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOMAIN);
                string str5 = dateTime.Minute.ToString();
                strArray6[9] = str5;
                strArray1[10] = ", ";
                string[] strArray7 = strArray1;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOMAIN);
                string str6 = dateTime.Second.ToString();
                strArray7[11] = str6;
                strArray1[12] = ")ORDER BY LASTUPDT DESC";
                cmdText1 = string.Concat(strArray1);
                string[] strArray8 = new string[13];
                strArray8[0] = " SELECT LOGNUM, CREWCODE, DUTYTYPE, LEGID, LASTUSER, LASTUPDT FROM pocrew WHERE LASTUPDT > DATETIME(";
                string[] strArray9 = strArray8;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOCREW);
                string str7 = dateTime.Year.ToString();
                strArray9[1] = str7;
                strArray8[2] = ", ";
                string[] strArray10 = strArray8;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOCREW);
                string str8 = dateTime.Month.ToString();
                strArray10[3] = str8;
                strArray8[4] = ", ";
                string[] strArray11 = strArray8;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOCREW);
                string str9 = dateTime.Day.ToString();
                strArray11[5] = str9;
                strArray8[6] = ", ";
                string[] strArray12 = strArray8;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOCREW);
                string str10 = dateTime.Hour.ToString();
                strArray12[7] = str10;
                strArray8[8] = ", ";
                string[] strArray13 = strArray8;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOCREW);
                string str11 = dateTime.Minute.ToString();
                strArray13[9] = str11;
                strArray8[10] = ", ";
                string[] strArray14 = strArray8;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOCREW);
                string str12 = dateTime.Second.ToString();
                strArray14[11] = str12;
                strArray8[12] = ")ORDER BY LASTUPDT DESC";
                cmdText2 = string.Concat(strArray8);
                string[] strArray15 = new string[13];
                strArray15[0] = " SELECT TECHLOG, DEPICAO_ID, ARRICAO_ID, CLIENT, SCHEDTTM, TIMEOFF, BLKOUT, FUEL_OUT, PAX_TOTAL, CAT_CODE, TIMEON, BLKIN, FUEL_IN, LEG_NUM, LEGID, LASTUPDT, LOGNUM, INDTTM, OUTDTTM FROM POLEGS WHERE LASTUPDT > DATETIME(";
                string[] strArray16 = strArray15;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOLEGS);
                string str13 = dateTime.Year.ToString();
                strArray16[1] = str13;
                strArray15[2] = ", ";
                string[] strArray17 = strArray15;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOLEGS);
                string str14 = dateTime.Month.ToString();
                strArray17[3] = str14;
                strArray15[4] = ", ";
                string[] strArray18 = strArray15;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOLEGS);
                string str15 = dateTime.Day.ToString();
                strArray18[5] = str15;
                strArray15[6] = ", ";
                string[] strArray19 = strArray15;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOLEGS);
                string str16 = dateTime.Hour.ToString();
                strArray19[7] = str16;
                strArray15[8] = ", ";
                string[] strArray20 = strArray15;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOLEGS);
                string str17 = dateTime.Minute.ToString();
                strArray20[9] = str17;
                strArray15[10] = ", ";
                string[] strArray21 = strArray15;
                dateTime = Convert.ToDateTime(DBALEMexJet.sUltimaCargaBitacorasPOLEGS);
                string str18 = dateTime.Second.ToString();
                strArray21[11] = str18;
                strArray15[12] = ")ORDER BY LASTUPDT DESC";
                cmdText3 = string.Concat(strArray15);
            }

                string sQueryMain = "SELECT * FROM OPENQUERY(" + MyGlobals.LinkedServer + ", '" + cmdText1 + "')";
                string sQueryCrew = "SELECT * FROM OPENQUERY(" + MyGlobals.LinkedServer + ", '" + cmdText2 + "')";
                string sQueryLegs = "SELECT * FROM OPENQUERY(" + MyGlobals.LinkedServer + ", '" + cmdText3 + "')";

                DataTable dtMain = new DBBaseFPK().oBD_SP.EjecutarDT_DeQuery(sQueryMain);
                DataTable dtCrew = new DBBaseFPK().oBD_SP.EjecutarDT_DeQuery(sQueryCrew);
                DataTable dtLegs = new DBBaseFPK().oBD_SP.EjecutarDT_DeQuery(sQueryLegs);

                dataSet.Tables.Add(dtMain);
                dataSet.Tables.Add(dtCrew);
                dataSet.Tables.Add(dtLegs);

                dataSet.Tables[0].TableName = "POMAIN";
                dataSet.Tables[1].TableName = "POCREW";
                dataSet.Tables[2].TableName = "POLEGS";

                return dataSet;
            }
            catch (Exception ex)
            {
            throw ex;
            }
        }
    }
}
