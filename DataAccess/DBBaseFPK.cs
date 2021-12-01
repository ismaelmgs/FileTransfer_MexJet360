using NucleoBase.BaseDeDatos;
using NucleoBase.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileTransfer_MexJet_360.DataAccess
{
    public class DBBaseFPK
    {
        public BD_SP oBD_SP = new BD_SP();

        public DBBaseFPK()
        {
            oBD_SP.sConexionSQL = Globales.GetConfigConnection("SqlDefaultFPK");
        }
    }
}
