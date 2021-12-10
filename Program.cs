// Decompiled with JetBrains decompiler
// Type: FileTransfer_MexJet_360.Program
// Assembly: FileTransfer_MexJet_360, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: 51C0F2EE-2D8C-4E2B-B102-38D0C4F03E12
// Assembly location: E:\AerolineasEjecutivas\Codigos\FileTransferMexJet360\ejecutable\FileTransfer_MexJet_360.exe

using FileTransfer_MexJet_360.Clases;
using FileTransfer_MexJet_360.DataAccess;
using System;
using System.IO;
using System.Reflection;

namespace FileTransfer_MexJet_360
{
    internal class Program
    {
        public static DBALEMexJet oDBALE = new DBALEMexJet();
        public static DBFlightPak oDBFli = new DBFlightPak();

        private static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*                        FILE TRANSFER  MEXJET  360                           *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("* Prevalidando registros anteriormente en error ...                           *");
                Program.oDBFli.CargarPreValidacionBitacoras();
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("* Realizando respaldo de la base de datos ...                                 *");

                //if (Program.oDBFli.RespaldarBaseDatos())
                //    Console.WriteLine("*Se realizo una copia exitosa                                                 *");
                //else
                //    Console.WriteLine("*Ocurrio un error al crear la copia                                           *");

                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*                     Conectando a las Bases de Datos                         *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*                                                                             *");

                if (Program.oDBALE.TestConnection())
                    Console.WriteLine("* Conexión Exitosa a base de datos ALEMexJet                                  *");
                else
                    Console.WriteLine("* Ocurrio un error al intentar conectar con la base de datos ALEMexJet        *");

                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");

                if (Program.oDBFli.TestConnection())
                    Console.WriteLine("* Conexión Exitosa con base de datos FlightPak                                *");
                else
                    Console.WriteLine("* Ocurrio un error al intentar conectar con la base de datos FlightPak        *");

                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*                   Inicia transferencia de registros                         *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*                     <---------- AEROPUERTO  ---------->                     *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*                                                                             *");

                if (Program.oDBALE.VerificarUltimoEnvioAeropuertos())
                    Console.WriteLine(string.Format("* Se Cargaran los registros posteriores a --><{0}>           *", (object)DBALEMexJet.sFechaLastUpdt_Aeropuertos));
                else
                    Console.WriteLine("* Se cargara toda la información proveniente de los bdf's de FlightPak        *");

                Console.WriteLine("* Cargando Registros ...                                                      *");
                Program.oDBFli.CargarRegistrosAeropuerto();
                Console.WriteLine("* Acción Exitosa                                                              *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*                       <---------- PILOTOS  ---------->                      *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*                                                                             *");

                if (Program.oDBALE.VerificarUltimoEnvioPilotos())
                    Console.WriteLine(string.Format("* Se Cargaran los registros posteriores a --><{0}>           *", (object)DBALEMexJet.sFechaLastUpdt_Pilotos));
                else
                    Console.WriteLine("* Se cargara toda la información proveniente de los bdf's de FlightPak        *");

                Console.WriteLine("*                                                                             *");
                Console.WriteLine("* Cargando Registros ...                                                      *");
                Program.oDBFli.CargarRegistrosPiloto();
                Console.WriteLine("* Acción Exitosa                                                              *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*        <---------- TRIPCREW - TRIPS SOLICITUD ---------->                   *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*                                                                             *");

                if (Program.oDBALE.VerificarUltimoEnvioTripCrew())
                    Console.WriteLine(string.Format("* Se Cargaran los registros posteriores a --><{0}>            *", (object)DBALEMexJet.sUltimaCargaTripCrew));
                else
                    Console.WriteLine("* Se cargara toda la información proveniente de los bdf's de FlightPak        *");

                Console.WriteLine("*                                                                             *");
                Console.WriteLine("* Cargando Registros ...                                                      *");
                Program.oDBFli.CargarRegistrosTripCrew();
                Console.WriteLine("* Acción Exitosa                                                              *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*                    <---------- BITACORAS ---------->                        *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*                                                                             *");

                if (Program.oDBALE.VerificarUltimoEnvioBitacoras())
                    Console.WriteLine(string.Format("* Se Cargaran los registros posteriores a --><{0}>            *", (object)DBALEMexJet.sUltimaCargaBitacorasPOMAIN));
                else
                    Console.WriteLine("* Se cargara toda la información proveniente de los bdf's de FlightPak        *");

                Console.WriteLine("*                                                                             *");
                Console.WriteLine("* Cargando Registros ...                                                      *");
                Program.oDBFli.CargarRegistrosBitacora();
                Console.WriteLine("* Acción Exitosa                                                              *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*             <---------- TRIPMAIN NOTAS TRIP---------->                      *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*                                                                             *");

                if (Program.oDBALE.VerificarUltimoEnvioTripMain())
                    Console.WriteLine(string.Format("* Se Cargaran los registros posteriores a --><{0}>            *", (object)DBALEMexJet.sUltimaCargaTripMain));
                else
                    Console.WriteLine("* Se cargara toda la información proveniente de los bdf's de FlightPak        *");

                Console.WriteLine("*                                                                             *");
                Console.WriteLine("* Cargando Registros ...                                                      *");
                Program.oDBFli.CargarRegistrosTripMain();
                Console.WriteLine("* Acción Exitosa                                                              *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*        <---------- TRIPLEG SOLICITUDES FLIGHTPACK ---------->               *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*                                                                             *");

                if (Program.oDBALE.VerificarUltimoEnvioTripLeg())
                    Console.WriteLine(string.Format("* Se Cargaran los registros posteriores a --><{0}>            *", (object)DBALEMexJet.sUltimaCargaTripLegs));
                else
                    Console.WriteLine("* Se cargara toda la información proveniente de los bdf's de FlightPak        *");

                Console.WriteLine("*                                                                             *");
                Console.WriteLine("* Cargando Registros                                                          *");
                Program.oDBFli.CargarRegistrosTripLeg();
                Console.WriteLine("* Acción Exitosa                                                              *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*                                                                             *");
                Console.WriteLine("*******************************************************************************");
                Console.WriteLine("*             !!!!   Proceso Finalizado Exitosamente   !!!!                   *");
                Console.WriteLine("*******************************************************************************");
                Utils.GuardarBitacora("!!!!   Proceso Finalizado Exitosamente   !!!!");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Utils.GuardarBitacora("Ocurrio un error: " + ex.Message + ", Paso: "+ MyGlobals.StepLog);
            }
        }

    }
}
