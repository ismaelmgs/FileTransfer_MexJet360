using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FileTransfer_MexJet_360.Clases
{
    public static class Utils
    {
        public static void GuardarBitacora(string sMensaje)
        {
            try
            {
                string path1 = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location) + "\\BitacorasApp\\";
                string str = "Bitacora_" + DateTime.Now.ToString("yyyy_MM_dd") + ".txt";
                if (!Directory.Exists(path1))
                    Directory.CreateDirectory(path1);
                string path2 = path1 + str;
                if (!File.Exists(path2))
                    File.CreateText(path2).Close();
                StreamWriter streamWriter = File.AppendText(path2);
                streamWriter.WriteLine(DateTime.Now.ToString("HH:mm:ss") + " - " + sMensaje);
                streamWriter.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}
