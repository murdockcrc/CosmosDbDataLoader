using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace PremiumTableGetStarted.Models
{
    class FlightFactModel: TableEntity
    {
        public int Year { get; set; }       
        public int Quarter { get; set; }
        public int Month { get; set; }
        public int DayOfMonth { get; set; }
        public int DayOfWeek { get; set; }
        public DateTime FlightDate { get; set; }
        public string UniqueCarrier { get; set; }
        public string AirlineID { get; set; }
        public string Carrier { get; set; }
        public string TailNum { get; set; }
        public string FlightNum { get; set; }
        public int OriginAirportID { get; set; }
        public int OriginAirportSeqID { get; set; }
        public int OriginCityMarketID { get; set; }
        public string Origin { get; set; }
        public string OriginCityName { get; set; }
        public string OriginState { get; set; }
        public string OriginStateFips { get; set; }
        public string OriginStateName { get; set; }
        public string OriginWac { get; set; }

        public FlightFactModel(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }
    }

}
